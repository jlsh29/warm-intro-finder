"""Finalize the Farcaster dataset for use by the warm-intro Flask app.

Pipeline this runs (all operating on files in farcaster_data/):

1. Read collection_progress.json + people.csv to get the kept FID set.
2. Re-emit edges.csv with:
     * tier=mutual, strength=8  for bidirectional follows
     * tier=follow, strength=4  for one-way follows (A->B but not B->A)
   (The app's core dedupes by max-strength so mutuals win when both exist.)
3. Insert the 'me' row into people.csv + identities.csv using the
   handles from user_profile.json.
4. Attach 'me' into the network with synthetic mutual edges to the
   seed FID and the top-N most-followed users in the kept set. This is
   what the user meant by "connect @Jel_29 to the network" — without
   their actual following list we use network hubs as an entry point.

Idempotent: safe to re-run. The 'me' row is replaced if it already exists.
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path


SEED_FID = 3  # dwr.eth — crawl root, always included as a me-neighbor.
N_HUBS = 10    # how many top-followed users 'me' is connected to.


def load_progress(path: Path) -> dict[int, set[int]]:
    with open(path, "r", encoding="utf-8") as f:
        d = json.load(f)
    return {int(k): set(v) for k, v in (d.get("seen") or {}).items()}


def load_kept_fids(people_path: Path) -> list[int]:
    fids: list[int] = []
    with open(people_path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            pid = (row.get("id") or "").strip()
            if pid.startswith("fc_"):
                try:
                    fids.append(int(pid.split("_", 1)[1]))
                except ValueError:
                    pass
    return fids


def load_identities(identities_path: Path) -> tuple[list[str], list[dict]]:
    with open(identities_path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        fieldnames = list(r.fieldnames or [])
        rows = list(r)
    return fieldnames, rows


def build_edges(seen: dict[int, set[int]], kept: set[int]
                ) -> list[tuple[int, int, int, str]]:
    """Return list of (from_fid, to_fid, strength, tier).

    An A->B arrow exists iff A is in seen (we fetched their following) and
    B is in A's follows AND B is in the kept set. If B->A also exists the
    pair is emitted once as mutual; otherwise once as a directed 'follow'.
    """
    mutual: set[tuple[int, int]] = set()   # sorted pair
    directed: set[tuple[int, int]] = set() # a -> b

    for a, follows_a in seen.items():
        if a not in kept:
            continue
        for b in follows_a:
            if b not in kept or b == a:
                continue
            directed.add((a, b))

    # Find pairs where both A->B and B->A exist.
    for a, b in list(directed):
        if (b, a) in directed:
            pair = (a, b) if a < b else (b, a)
            mutual.add(pair)

    out: list[tuple[int, int, int, str]] = []
    mutual_members: set[tuple[int, int]] = set()
    for a, b in sorted(mutual):
        out.append((a, b, 8, "mutual"))
        mutual_members.add((a, b))
        mutual_members.add((b, a))

    for a, b in sorted(directed):
        if (a, b) in mutual_members:
            continue
        out.append((a, b, 4, "follow"))

    return out


def pick_hubs(identities_rows: list[dict], n: int) -> list[str]:
    """Top-N kept person_ids by farcaster_followers count."""
    def _followers(row: dict) -> int:
        try:
            return int(row.get("farcaster_followers") or 0)
        except (TypeError, ValueError):
            return 0
    sorted_rows = sorted(identities_rows, key=_followers, reverse=True)
    return [r["person_id"] for r in sorted_rows[:n] if r.get("person_id")]


def write_people(path: Path, fids: list[int], include_me: bool) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id"])
        if include_me:
            w.writerow(["me"])
        for fid in fids:
            w.writerow([f"fc_{fid}"])


def write_edges(path: Path, edges: list[tuple[int, int, int, str]],
                me_neighbors: list[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["from", "to", "strength", "tier"])
        for nb in me_neighbors:
            w.writerow(["me", nb, 8, "mutual"])
        for a, b, s, tier in edges:
            w.writerow([f"fc_{a}", f"fc_{b}", s, tier])


def write_identities(path: Path, fieldnames: list[str], rows: list[dict],
                     me_row: dict) -> None:
    # Drop any existing 'me' row, then prepend the fresh one.
    filtered = [r for r in rows if (r.get("person_id") or "").strip() != "me"]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerow(me_row)
        for r in filtered:
            w.writerow(r)


def build_me_row(fieldnames: list[str], profile: dict, today: str) -> dict:
    twitter = (profile.get("twitter") or "").strip()
    if twitter and not twitter.startswith("@"):
        twitter = f"@{twitter}"
    row = {k: "" for k in fieldnames}
    row.update({
        "person_id": "me",
        "twitter": twitter,
        "farcaster": profile.get("farcaster") or "",
        "linkedin": profile.get("linkedin") or "",
        "debank": profile.get("debank") or "",
        "twitter_likes": "3",
        "twitter_comments": "0",
        "twitter_reposts": "7",
        "farcaster_recasts": "2",
        "farcaster_replies": "10",
        "linkedin_recommendations": "0",
        "linkedin_endorsements": "0",
        "debank_transactions": "0",
        "twitter_followers": "347",
        "farcaster_followers": "508",
        "linkedin_connections": "107",
        "debank_followers": "30",
        "last_interaction": today,
        "interaction_score": "8",
    })
    # Drop keys not in this schema.
    return {k: row.get(k, "") for k in fieldnames}


def main() -> int:
    import argparse
    from datetime import date

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-dir", type=Path, default=Path("farcaster_data"))
    p.add_argument("--progress", type=Path,
                   default=Path("collection_progress.json"))
    p.add_argument("--profile", type=Path, default=Path("user_profile.json"))
    p.add_argument("--hubs", type=int, default=N_HUBS)
    args = p.parse_args()

    people_csv = args.data_dir / "people.csv"
    edges_csv = args.data_dir / "edges.csv"
    identities_csv = args.data_dir / "identities.csv"

    for f in (people_csv, edges_csv, identities_csv, args.progress, args.profile):
        if not f.exists():
            print(f"[error] missing {f}", file=sys.stderr)
            return 1

    seen = load_progress(args.progress)
    kept_fids = load_kept_fids(people_csv)
    kept_set = set(kept_fids)
    print(f"[kept] {len(kept_fids)} Farcaster users")

    # Build edges.
    edges = build_edges(seen, kept_set)
    mutuals = sum(1 for *_, tier in edges if tier == "mutual")
    follows = sum(1 for *_, tier in edges if tier == "follow")
    print(f"[edges] {mutuals} mutual + {follows} follow = {len(edges)} total")

    # Add 'me' row + synthetic edges to seed + top hubs.
    fieldnames, identities_rows = load_identities(identities_csv)
    with open(args.profile, "r", encoding="utf-8") as f:
        profile = json.load(f)
    today = date.today().isoformat()
    me_row = build_me_row(fieldnames, profile, today)

    hubs = pick_hubs(identities_rows, args.hubs)
    seed_id = f"fc_{SEED_FID}"
    me_neighbors: list[str] = []
    seen_neighbors: set[str] = set()
    # Always include seed first.
    if seed_id in {f"fc_{fid}" for fid in kept_fids}:
        me_neighbors.append(seed_id)
        seen_neighbors.add(seed_id)
    for pid in hubs:
        if pid not in seen_neighbors:
            me_neighbors.append(pid)
            seen_neighbors.add(pid)
    print(f"[me] connecting 'me' to {len(me_neighbors)} hubs "
          f"(seed + top-{args.hubs} followed)")

    write_people(people_csv, kept_fids, include_me=True)
    write_edges(edges_csv, edges, me_neighbors)
    write_identities(identities_csv, fieldnames, identities_rows, me_row)

    print(f"[write] {people_csv} ({len(kept_fids) + 1} people incl. me)")
    print(f"[write] {edges_csv} ({len(edges) + len(me_neighbors)} edges)")
    print(f"[write] {identities_csv} ({len(identities_rows) + 1} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
