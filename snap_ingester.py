"""Stanford SNAP ego-Twitter -> warm-intro CSV format.

Downloads the Stanford SNAP ego-Twitter dataset (81,306 real anonymized
Twitter users, ~1.7M directed follow edges, research-licensed free
download), subsamples to a connected ~5k-node subgraph for in-memory
pathfinding performance, and emits the engine's three-CSV format.

The dataset is ANONYMIZED — node ids are integers, not real handles.
We emit person ids as `tw_<numeric>` and identities handles as
`@user_<numeric>` to make clear these are pseudonyms while still
flowing through the existing twitter_ingester-compatible pipeline.

Usage
-----
    python snap_ingester.py --out-dir snap_data

Then swap into the live dataset:
    mv people.csv people.synthetic.bak && cp snap_data/people.csv .
    mv edges.csv  edges.synthetic.bak  && cp snap_data/edges.csv  .
    mv identities.csv identities.synthetic.bak && cp snap_data/identities.csv .
    echo snap-twitter > .dataset_source
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import tarfile
import urllib.request
from collections import defaultdict, deque
from pathlib import Path

SNAP_URL = "https://snap.stanford.edu/data/twitter.tar.gz"

# Identities.csv columns must exactly match the live-dataset shape so
# existing readers in app.py / extras.py / core.py don't see surprises.
IDENTITIES_HEADER = [
    "person_id", "twitter", "farcaster", "linkedin", "debank",
    "twitter_likes", "twitter_comments", "twitter_reposts",
    "farcaster_recasts", "farcaster_replies",
    "linkedin_recommendations", "linkedin_endorsements",
    "debank_transactions",
    "twitter_followers", "farcaster_followers",
    "linkedin_connections", "debank_followers",
    "last_interaction", "interaction_score",
]


def download(url: str, dest: Path) -> None:
    if dest.exists() and dest.stat().st_size > 0:
        print(f"[skip] {dest} already present ({dest.stat().st_size // 1024} KB)")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"[download] {url} -> {dest}")
    with urllib.request.urlopen(url) as resp, open(dest, "wb") as out:
        total = int(resp.headers.get("Content-Length") or 0)
        read = 0
        chunk = 64 * 1024
        while True:
            buf = resp.read(chunk)
            if not buf:
                break
            out.write(buf)
            read += len(buf)
            if total:
                pct = read * 100 // total
                sys.stdout.write(f"\r  {read // 1024} / {total // 1024} KB ({pct}%)")
                sys.stdout.flush()
        sys.stdout.write("\n")


def extract(archive: Path, out_dir: Path) -> Path:
    marker = out_dir / ".extracted"
    if marker.exists():
        print(f"[skip] already extracted to {out_dir}")
    else:
        out_dir.mkdir(parents=True, exist_ok=True)
        print(f"[extract] {archive} -> {out_dir}")
        with tarfile.open(archive, "r:gz") as tar:
            tar.extractall(out_dir)
        marker.write_text("done\n")
    # SNAP archives extract into `twitter/` subdir.
    tw_dir = out_dir / "twitter"
    if not tw_dir.is_dir():
        raise RuntimeError(f"Expected {tw_dir} after extract, not found")
    return tw_dir


def load_directed_follows(raw_dir: Path) -> set[tuple[str, str]]:
    """Union of all directed follow edges across every ego network."""
    edges: set[tuple[str, str]] = set()
    edge_files = sorted(raw_dir.glob("*.edges"))
    print(f"[parse] reading {len(edge_files)} .edges files")
    for i, ef in enumerate(edge_files, 1):
        with open(ef, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) != 2:
                    continue
                a, b = parts
                if a == b:
                    continue
                edges.add((a, b))
        if i % 100 == 0:
            print(f"  {i}/{len(edge_files)} files, {len(edges):,} edges so far")
    print(f"[parse] total directed edges: {len(edges):,}")
    return edges


def largest_weakly_connected(nodes: set[str],
                             adj: dict[str, set[str]]) -> set[str]:
    """Return the node set of the largest weakly-connected component."""
    visited: set[str] = set()
    best: set[str] = set()
    for seed in nodes:
        if seed in visited:
            continue
        comp: set[str] = set()
        q: deque[str] = deque([seed])
        while q:
            n = q.popleft()
            if n in comp:
                continue
            comp.add(n)
            for nb in adj.get(n, ()):
                if nb not in comp:
                    q.append(nb)
        visited |= comp
        if len(comp) > len(best):
            best = comp
    return best


def subsample_by_degree(nodes: set[str],
                        directed: set[tuple[str, str]],
                        cap: int) -> set[str]:
    """Keep the `cap` highest-total-degree nodes."""
    if len(nodes) <= cap:
        return nodes
    deg: defaultdict[str, int] = defaultdict(int)
    for a, b in directed:
        if a in nodes:
            deg[a] += 1
        if b in nodes:
            deg[b] += 1
    ranked = sorted(nodes, key=lambda n: deg.get(n, 0), reverse=True)
    return set(ranked[:cap])


def build_csvs(directed: set[tuple[str, str]],
               include_one_way: bool,
               max_nodes: int) -> tuple[list[str], list[tuple[str, str, int, str]]]:
    """Return (people_ids, edge_rows) after mutual collapse + subsampling."""
    # Adjacency for connectivity / degree.
    undir_adj: defaultdict[str, set[str]] = defaultdict(set)
    for a, b in directed:
        undir_adj[a].add(b)
        undir_adj[b].add(a)
    all_nodes = set(undir_adj)
    print(f"[graph] unique nodes: {len(all_nodes):,}")

    lwcc = largest_weakly_connected(all_nodes, undir_adj)
    print(f"[graph] largest weakly-connected component: {len(lwcc):,}")

    kept = subsample_by_degree(lwcc, directed, max_nodes)
    print(f"[graph] subsampled to top {len(kept):,} by degree")

    # Emit edges between KEPT nodes only.
    edge_rows: list[tuple[str, str, int, str]] = []
    # Canonicalize pair -> (tier, strength). Mutual wins over one-way.
    seen: dict[frozenset[str], tuple[int, str]] = {}
    for a, b in directed:
        if a not in kept or b not in kept:
            continue
        key = frozenset((a, b))
        reverse = (b, a) in directed
        if reverse:
            seen[key] = (8, "mutual")
        elif include_one_way and key not in seen:
            seen[key] = (2, "platform_similarity")
    for pair, (strength, tier) in seen.items():
        a, b = sorted(pair)
        edge_rows.append((f"tw_{a}", f"tw_{b}", strength, tier))

    # People list: only those that have at least one surviving edge
    # (otherwise they're dangling isolates after one-way drop).
    touched: set[str] = set()
    for f, t, _, _ in edge_rows:
        touched.add(f)
        touched.add(t)
    people = sorted(touched)
    print(f"[graph] emitted {len(people):,} people, {len(edge_rows):,} edges")
    return people, edge_rows


def write_csvs(out_dir: Path, people: list[str],
               edge_rows: list[tuple[str, str, int, str]]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / "people.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id"])
        for pid in people:
            w.writerow([pid])
    with open(out_dir / "edges.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["from", "to", "strength", "tier"])
        for row in edge_rows:
            w.writerow(row)
    with open(out_dir / "identities.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(IDENTITIES_HEADER)
        for pid in people:
            num = pid[len("tw_"):]
            row = [pid, f"@user_{num}", "", "", ""]
            row += ["0"] * 8            # twitter_* / farcaster_* / linkedin_* / debank_tx
            row += ["0", "0", "0", "0"] # *_followers / connections
            row += ["", "0"]            # last_interaction (blank), interaction_score
            w.writerow(row)
    print(f"[write] {out_dir}/people.csv  edges.csv  identities.csv")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out-dir", default="snap_data", type=Path,
                   help="Directory for the emitted CSVs (default: snap_data)")
    p.add_argument("--raw-dir", default="snap_raw", type=Path,
                   help="Staging directory for the downloaded archive + extract")
    p.add_argument("--max-nodes", type=int, default=5000,
                   help="Cap on nodes in the subsampled graph")
    p.add_argument("--include-one-way", action="store_true",
                   help="Also emit one-way follows at strength 2 (default: mutuals only)")
    args = p.parse_args()

    archive = args.raw_dir / "twitter.tar.gz"
    download(SNAP_URL, archive)
    tw_dir = extract(archive, args.raw_dir)
    directed = load_directed_follows(tw_dir)
    people, edges = build_csvs(directed, args.include_one_way, args.max_nodes)
    write_csvs(args.out_dir, people, edges)
    print("[done] drop these CSVs into the project root to make them live.")


if __name__ == "__main__":
    main()
