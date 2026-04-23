"""Rebuild farcaster_data/*.csv from the current collection_progress.json.

Why this exists: the Warpcast API is rate-limiting the crawler hard, so
we stop trying to push `seen` from 3,176 → 5,000 and instead use all the
data we've already collected. The progress file has 25,877 *discovered*
profiles plus 3,176 fully-crawled follow lists — more than enough real
data for a 5,000-person dataset.

Picks 5,000 FIDs with this priority:
  1. Every fid we fully crawled (seen list, up to 3,176).
  2. Fill the remainder with discovered profiles, highest followerCount
     first, preferring those with usernames.

Writes people.csv / identities.csv / edges.csv in farcaster_data/ using
the schema expected by the warm-intro Flask app. Edges include:
  * mutual (strength 8) where both A->B and B->A were observed
  * follow (strength 4) where only one direction was observed
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import date
from pathlib import Path


TARGET = 5000

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


def pick_kept(seen: dict[int, set[int]], profiles: dict[int, dict],
              target: int) -> list[int]:
    # 1) Start with every crawled user that has a profile entry.
    kept: list[int] = []
    seen_set = set()
    for fid in seen.keys():
        if fid in profiles:
            kept.append(fid)
            seen_set.add(fid)

    # 2) Fill the remainder with discovered-only profiles, most-followed first,
    #    preferring those with usernames (real-handle requirement).
    remainder = target - len(kept)
    if remainder > 0:
        candidates = [
            (fid, p) for fid, p in profiles.items()
            if fid not in seen_set and (p.get("username") or "").strip()
        ]
        candidates.sort(key=lambda kv: int(kv[1].get("followerCount") or 0),
                        reverse=True)
        for fid, _ in candidates[:remainder]:
            kept.append(fid)
    return kept[:target]


def build_edges(seen: dict[int, set[int]], kept: set[int]
                ) -> list[tuple[int, int, int, str]]:
    directed: set[tuple[int, int]] = set()
    for a, follows_a in seen.items():
        if a not in kept:
            continue
        for b in follows_a:
            if b not in kept or b == a:
                continue
            directed.add((a, b))

    mutual_pairs: set[tuple[int, int]] = set()
    for a, b in directed:
        if (b, a) in directed:
            lo, hi = (a, b) if a < b else (b, a)
            mutual_pairs.add((lo, hi))

    out: list[tuple[int, int, int, str]] = []
    for a, b in sorted(mutual_pairs):
        out.append((a, b, 8, "mutual"))
    mutual_members = {(a, b) for pair in mutual_pairs for a, b in (pair, pair[::-1])}
    for a, b in sorted(directed):
        if (a, b) in mutual_members:
            continue
        out.append((a, b, 4, "follow"))
    return out


def write_people(path: Path, fids: list[int]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id"])
        for fid in fids:
            w.writerow([f"fc_{fid}"])


def write_identities(path: Path, fids: list[int],
                     profiles: dict[int, dict], today: str) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(IDENTITIES_HEADER)
        for fid in fids:
            p = profiles.get(fid) or {}
            handle = (p.get("username") or "").strip()
            if handle and not handle.startswith("@"):
                handle = f"@{handle}"
            row = [f"fc_{fid}", "", handle, "", "",
                   "0", "0", "0",
                   "0", "0",
                   "0", "0",
                   "0",
                   "0",
                   str(p.get("followerCount") or 0),
                   "0", "0",
                   today, "0"]
            w.writerow(row)


def write_edges_csv(path: Path, edges: list[tuple[int, int, int, str]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["from", "to", "strength", "tier"])
        for a, b, s, tier in edges:
            w.writerow([f"fc_{a}", f"fc_{b}", s, tier])


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--progress", type=Path,
                   default=Path("collection_progress.json"))
    p.add_argument("--out-dir", type=Path, default=Path("farcaster_data"))
    p.add_argument("--target", type=int, default=TARGET)
    args = p.parse_args()

    if not args.progress.exists():
        print(f"[error] missing progress file {args.progress}", file=sys.stderr)
        return 1

    with open(args.progress, "r", encoding="utf-8") as f:
        d = json.load(f)
    seen = {int(k): set(int(x) for x in v)
            for k, v in (d.get("seen") or {}).items() if v}
    profiles = {int(k): v for k, v in (d.get("profiles") or {}).items()}
    print(f"[load] seen (with follow data)={len(seen)} profiles={len(profiles)}")

    fids = pick_kept(seen, profiles, args.target)
    print(f"[pick] {len(fids)} kept fids")
    kept = set(fids)

    edges = build_edges(seen, kept)
    mutuals = sum(1 for *_, t in edges if t == "mutual")
    follows = sum(1 for *_, t in edges if t == "follow")
    print(f"[edges] {mutuals} mutual + {follows} follow = {len(edges)} total")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    today = date.today().isoformat()
    write_people(args.out_dir / "people.csv", fids)
    write_identities(args.out_dir / "identities.csv", fids, profiles, today)
    write_edges_csv(args.out_dir / "edges.csv", edges)
    print(f"[write] {args.out_dir}/people.csv ({len(fids)} rows)")
    print(f"[write] {args.out_dir}/identities.csv ({len(fids)} rows)")
    print(f"[write] {args.out_dir}/edges.csv ({len(edges)} edges)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
