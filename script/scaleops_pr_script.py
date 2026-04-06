"""
create_resource_pr.py
─────────────────────────────────────────────────────────────────────────────
Reads live resource values from Pods (as mutated by the ScaleOps webhook),
traces each Pod back to its owner Deployment/StatefulSet, then opens a GitHub
PR that patches the matching YAML manifests with those values.

The idea: ScaleOps already right-sizes pods at runtime — this script "bakes"
those values into the source manifests so the owner can own them permanently.

Requirements:
    pip install kubernetes PyGithub ruamel.yaml

Usage:
    python create_resource_pr.py \
        --github-token  ghp_xxxx \
        --repo          org/their-repo \
        --base-branch   main \
        --manifests-dir k8s/ \
        --namespace     production
"""

import argparse
import base64
import sys
from collections import defaultdict
from io import StringIO
from pathlib import PurePosixPath

from github import Github, GithubException
from kubernetes import client, config
from ruamel.yaml import YAML

# ─── YAML setup ───────────────────────────────────────────────────────────────

yaml = YAML()
yaml.preserve_quotes = True
yaml.width = 4096  # prevent unwanted line-wrapping


# ─── Step 1: Connect to Kubernetes ────────────────────────────────────────────

def load_kube_config():
    try:
        config.load_incluster_config()
        print("[k8s] Using in-cluster config")
    except config.ConfigException:
        config.load_kube_config()
        print("[k8s] Using local kubeconfig (~/.kube/config)")


# ─── Step 2: Read live Pod resources + trace to owner ─────────────────────────

def get_owner_ref(pod) -> tuple[str, str] | None:
    """
    Walk the owner chain of a Pod up to the top-level workload.
    Pod → ReplicaSet → Deployment  (or)
    Pod → StatefulSet

    Returns (kind, name) of the top-level owner, or None if not found.
    """
    apps_v1 = client.AppsV1Api()
    namespace = pod.metadata.namespace
    refs = pod.metadata.owner_references or []

    for ref in refs:
        if ref.kind == "StatefulSet":
            return "StatefulSet", ref.name

        if ref.kind == "ReplicaSet":
            # Walk up: ReplicaSet → Deployment
            rs = apps_v1.read_namespaced_replica_set(ref.name, namespace)
            for rs_ref in rs.metadata.owner_references or []:
                if rs_ref.kind == "Deployment":
                    return "Deployment", rs_ref.name

        if ref.kind == "Deployment":
            return "Deployment", ref.name

    return None


def parse_cpu_to_millicores(cpu: str) -> int:
    """Convert a k8s CPU string to millicores (e.g. '200m' → 200, '1' → 1000)."""
    if cpu.endswith("m"):
        return int(cpu[:-1])
    return int(float(cpu) * 1000)


def parse_memory_to_bytes(mem: str) -> int:
    """Convert a k8s memory string to bytes (e.g. '256Mi' → 268435456)."""
    units = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40,
             "K":  1e3,   "M":  1e6,   "G":  1e9,   "T":  1e12}
    for suffix, multiplier in units.items():
        if mem.endswith(suffix):
            return int(float(mem[:-len(suffix)]) * multiplier)
    return int(mem)  # plain bytes


def format_cpu(millicores: int) -> str:
    """Format millicores back to a k8s CPU string."""
    if millicores >= 1000 and millicores % 1000 == 0:
        return str(millicores // 1000)
    return f"{millicores}m"


def format_memory(total_bytes: int) -> str:
    """Format bytes back to the most readable k8s memory string."""
    for suffix, divisor in [("Gi", 2**30), ("Mi", 2**20), ("Ki", 2**10)]:
        if total_bytes % divisor == 0:
            return f"{total_bytes // divisor}{suffix}"
    return str(total_bytes)


def collect_pod_resources(namespace: str) -> dict[tuple[str, str], dict[str, dict]]:
    """
    List all Running pods, group by owner workload, and AVERAGE the resource
    requests across all pods in the same workload (per container).

    This handles the case where ScaleOps is mid-rollout and different pods
    carry slightly different mutated values.

    Returns:
        {
            ("Deployment", "my-app"): {
                "app":     {"cpu": "210m", "memory": "270Mi"},
                "sidecar": {"cpu": "52m",  "memory": "66Mi"},
            },
        }
    """
    core_v1 = client.CoreV1Api()
    pods = core_v1.list_namespaced_pod(namespace)

    # raw accumulator: owner → container → list of (cpu_m, mem_bytes)
    raw: dict[tuple[str, str], dict[str, list[tuple[int, int]]]] = defaultdict(lambda: defaultdict(list))

    for pod in pods.items:
        phase = pod.status.phase if pod.status else None
        if phase != "Running":
            continue
        owner = get_owner_ref(pod)
        if not owner:
            continue

        for c in pod.spec.containers:
            requests = (c.resources.requests or {}) if c.resources else {}
            cpu_str = requests.get("cpu")
            mem_str = requests.get("memory")
            if cpu_str and mem_str:
                raw[owner][c.name].append((
                    parse_cpu_to_millicores(cpu_str),
                    parse_memory_to_bytes(mem_str),
                ))

    # Average per container
    averaged: dict[tuple[str, str], dict[str, dict]] = {}
    for owner, containers in raw.items():
        averaged[owner] = {}
        for cname, samples in containers.items():
            avg_cpu = round(sum(s[0] for s in samples) / len(samples))
            avg_mem = round(sum(s[1] for s in samples) / len(samples))
            averaged[owner][cname] = {
                "cpu":    format_cpu(avg_cpu),
                "memory": format_memory(avg_mem),
            }
        print(f"[k8s] {owner[0]}/{owner[1]} — averaged across {max(len(v) for v in containers.values())} pod(s): "
              f"{averaged[owner]}")

    print(f"[k8s] Total workloads: {len(averaged)}")
    return averaged


# ─── Step 3: Patch YAML manifests ─────────────────────────────────────────────

def patch_manifest(
    content: str,
    workload_resources: dict[tuple[str, str], dict[str, dict]],
) -> tuple[str | None, list[str]]:
    """
    Parse one YAML file (may contain multiple docs), update container resource
    requests for any Deployment/StatefulSet that matches a live workload.

    Returns (patched_yaml_string, human_readable_changes) or (None, []).
    """
    docs = list(yaml.load_all(StringIO(content)))
    changes = []

    for doc in docs:
        if not isinstance(doc, dict):
            continue
        kind = doc.get("kind", "")
        if kind not in ("Deployment", "StatefulSet"):
            continue

        name = doc.get("metadata", {}).get("name", "")
        key  = (kind, name)
        res_map = workload_resources.get(key)
        if not res_map:
            continue

        containers = (
            doc.get("spec", {})
               .get("template", {})
               .get("spec", {})
               .get("containers", [])
        )
        for container in containers:
            cname = container.get("name", "")
            live  = res_map.get(cname)
            if not live:
                continue

            resources = container.setdefault("resources", {})
            requests  = resources.setdefault("requests", {})

            old_cpu = requests.get("cpu",    "<unset>")
            old_mem = requests.get("memory", "<unset>")

            if live.get("cpu"):
                requests["cpu"]    = live["cpu"]
            if live.get("memory"):
                requests["memory"] = live["memory"]

            changes.append(
                f"  • `{name}/{cname}`: "
                f"cpu `{old_cpu}` → `{live.get('cpu', old_cpu)}`, "
                f"memory `{old_mem}` → `{live.get('memory', old_mem)}`"
            )

    if not changes:
        return None, []

    out = StringIO()
    yaml.dump_all(docs, out)
    return out.getvalue(), changes


# ─── Step 4: GitHub helpers ────────────────────────────────────────────────────

def get_or_create_branch(repo, base_branch: str, new_branch: str):
    try:
        repo.get_branch(new_branch)
        print(f"[gh] Branch '{new_branch}' already exists — reusing it")
    except GithubException:
        sha = repo.get_branch(base_branch).commit.sha
        repo.create_git_ref(ref=f"refs/heads/{new_branch}", sha=sha)
        print(f"[gh] Created branch '{new_branch}' off '{base_branch}'")


def commit_file(repo, path: str, content: str, branch: str, message: str):
    try:
        existing = repo.get_contents(path, ref=branch)
        repo.update_file(path, message, content, existing.sha, branch=branch)
        print(f"[gh]   Updated {path}")
    except GithubException:
        repo.create_file(path, message, content, branch=branch)
        print(f"[gh]   Created {path}")


def build_pr_body(all_changes: dict[str, list[str]], namespace: str) -> str:
    lines = [
        "## 📉 Right-Sizing Resource Requests",
        "",
        f"This PR was auto-generated from **live Pod resource values** in the `{namespace}` namespace.",
        "These are the values ScaleOps is already applying at runtime via its mutating webhook.",
        "",
        "The goal is to **bake them into your manifests** so your declared config matches",
        "what's actually running — no surprises, no drift.",
        "",
        "We'd love to walk you through the numbers before you merge. 🙏",
        "",
        "### Changes by file",
    ]
    for file, changes in all_changes.items():
        lines.append(f"\n**`{file}`**")
        lines.extend(changes)
    lines += [
        "",
        "---",
        "_Generated by `create_resource_pr.py` · source: live ScaleOps-mutated Pod specs_",
    ]
    return "\n".join(lines)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="PR generator: bakes ScaleOps live pod resources into YAML manifests"
    )
    parser.add_argument("--github-token",  required=True, help="GitHub PAT with repo write access")
    parser.add_argument("--repo",          required=True, help="Target repo in 'org/name' format")
    parser.add_argument("--base-branch",   default="main")
    parser.add_argument("--pr-branch",     default="scaleops/right-size-resources")
    parser.add_argument("--manifests-dir", default="k8s",
                        help="Path inside the repo to search for YAML manifests")
    parser.add_argument("--namespace",     default="default",
                        help="Kubernetes namespace to read live pod specs from")
    args = parser.parse_args()

    # ── 1. Kubernetes ──────────────────────────────────────────────────────────
    load_kube_config()
    workload_resources = collect_pod_resources(args.namespace)
    if not workload_resources:
        print("[!] No live pod resources collected — nothing to do.")
        sys.exit(0)

    # ── 2. GitHub ──────────────────────────────────────────────────────────────
    gh   = Github(args.github_token)
    repo = gh.get_repo(args.repo)
    print(f"[gh] Connected to {repo.full_name}")
    get_or_create_branch(repo, args.base_branch, args.pr_branch)

    # ── 3. Walk manifests and patch ────────────────────────────────────────────
    all_changes: dict[str, list[str]] = {}

    def walk(path: str):
        try:
            items = repo.get_contents(path, ref=args.base_branch)
        except GithubException as e:
            print(f"[gh] Cannot read '{path}': {e}")
            return
        for item in (items if isinstance(items, list) else [items]):
            if item.type == "dir":
                walk(item.path)
            elif item.path.endswith((".yaml", ".yml")):
                raw     = item.decoded_content.decode()
                patched, changes = patch_manifest(raw, workload_resources)
                if patched:
                    commit_file(
                        repo, item.path, patched, args.pr_branch,
                        f"chore: right-size resources in {PurePosixPath(item.path).name}",
                    )
                    all_changes[item.path] = changes

    print(f"\n[gh] Scanning '{args.manifests_dir}' for manifests to patch...")
    walk(args.manifests_dir)

    if not all_changes:
        print("[!] No manifest matched a live workload — no PR created.")
        sys.exit(0)

    # ── 4. Open PR ────────────────────────────────────────────────────────────
    pr = repo.create_pull(
        title="chore: right-size k8s resource requests to match ScaleOps live values",
        body=build_pr_body(all_changes, args.namespace),
        head=args.pr_branch,
        base=args.base_branch,
    )
    print(f"\n✅ PR opened: {pr.html_url}")


if __name__ == "__main__":
    main()
