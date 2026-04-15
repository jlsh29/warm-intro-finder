"""Twitter follower/following CSV -> warm_intro people.csv / edges.csv / identities.csv.

Usage
-----
    python twitter_ingester.py \
        --seed @alice \
        --following alice_following.csv \
        --followers alice_followers.csv \
        --seed @bob \
        --following bob_following.csv \
        --followers bob_followers.csv \
        --out-dir ./twitter_data

    # Then route through the existing engine:
    python warm_intro.py \
        --people twitter_data/people.csv \
        --edges twitter_data/edges.csv \
        --identities twitter_data/identities.csv \
        --entry tw_alice --target tw_charlie

Input format
------------
Each `--following` / `--followers` file is a CSV with at least a handle
column. Common column names auto-detected:
  username | handle | screen_name | user_screen_name | twitter_handle
Optional display-name column auto-detected:
  display_name | name | full_name | user_name
Other columns are ignored. Handles are lowercased and stripped of `@`.

Direction handling
------------------
Follows are inherently *directed* (A->B does not imply B->A). Warm intros
require mutual acquaintance, so by default this script emits an edge
between a seed and another handle ONLY when the follow is mutual
(i.e. handle appears in both the seed's following AND followers files).
Pass `--include-one-way` to also emit one-way edges (with lower strength).

Output
------
Three CSVs in `--out-dir`:
  people.csv       (id, name)
  edges.csv        (from, to, strength, tier)
  identities.csv   (person_id, platform, handle)

Person ids are namespaced as `tw_{handle}` so they don't collide with
ids from other platforms when you later merge sources via Phase B's
identity layer.

Safety
------
This script does not contact Twitter or fabricate edges. It only emits
what is present in the input CSVs.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict


# Column-name aliases used by various third-party Twitter export tools.
HANDLE_COLS = (
    "username",
    "handle",
    "screen_name",
    "user_screen_name",
    "twitter_handle",
)
NAME_COLS = ("display_name", "name", "full_name", "user_name")

# Edge strengths per tier (mirrors core.TIER_STRENGTH so this script
# remains independent of the engine module).
TIER_STRENGTH: dict[str, int] = {
    "direct": 10,
    "mutual": 8,
    "shared_org": 5,
    "platform_similarity": 2,
}


def detect_col(headers: list[str], candidates: tuple[str, ...]) -> str | None:
    headers_lower = {h.lower().strip(): h for h in headers}
    for cand in candidates:
        if cand in headers_lower:
            return headers_lower[cand]
    return None


def normalize_handle(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("@"):
        s = s[1:]
    return s.lower()


def person_id(handle: str) -> str:
    return f"tw_{handle}"


def read_handles_csv(path: str) -> dict[str, str]:
    """Read a CSV of Twitter accounts; return {handle: display_name}."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"input file not found: {path}")
    handles: dict[str, str] = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"{path}: empty CSV")
        handle_col = detect_col(reader.fieldnames, HANDLE_COLS)
        if not handle_col:
            raise ValueError(
                f"{path}: no handle column. Looked for one of {HANDLE_COLS}; "
                f"got {reader.fieldnames}"
            )
        name_col = detect_col(reader.fieldnames, NAME_COLS)
        for row in reader:
            h = normalize_handle(row.get(handle_col, ""))
            if not h:
                continue
            display = (row.get(name_col, "") or "").strip() if name_col else ""
            # Keep the longest non-empty display name we see for this handle.
            existing = handles.get(h, "")
            if not existing or (display and len(display) > len(existing)):
                handles[h] = display
    return handles


def ingest(
    seeds: list[tuple[str, str, str]],  # (handle, following_path, followers_path)
    out_dir: str,
    include_one_way: bool,
    edge_tier: str,
    one_way_tier: str = "platform_similarity",
) -> dict:
    if edge_tier not in TIER_STRENGTH:
        raise ValueError(f"unknown tier {edge_tier!r}")
    if one_way_tier not in TIER_STRENGTH:
        raise ValueError(f"unknown one-way tier {one_way_tier!r}")

    # handle -> display_name (longest seen)
    all_handles: dict[str, str] = {}

    # Edges keyed by sorted (handle_a, handle_b) -> (strength, tier)
    edges: dict[tuple[str, str], tuple[int, str]] = {}

    seen_seeds: set[str] = set()
    for seed_raw, following_path, followers_path in seeds:
        seed = normalize_handle(seed_raw)
        if not seed:
            raise ValueError(f"empty --seed value")
        if seed in seen_seeds:
            print(f"warning: seed {seed!r} listed twice; ignoring duplicate",
                  file=sys.stderr)
            continue
        seen_seeds.add(seed)

        following = read_handles_csv(following_path)
        followers = read_handles_csv(followers_path)
        # Strip self-references defensively.
        following.pop(seed, None)
        followers.pop(seed, None)

        # Merge handle->display info from both files (and the seed itself).
        all_handles.setdefault(seed, "")
        for h, n in following.items():
            cur = all_handles.get(h, "")
            if not cur or (n and len(n) > len(cur)):
                all_handles[h] = n
        for h, n in followers.items():
            cur = all_handles.get(h, "")
            if not cur or (n and len(n) > len(cur)):
                all_handles[h] = n

        following_set = set(following)
        followers_set = set(followers)
        mutual = following_set & followers_set
        only_following = following_set - followers_set
        only_followers = followers_set - following_set

        for other in mutual:
            key = tuple(sorted([seed, other]))
            new_s = TIER_STRENGTH[edge_tier]
            cur = edges.get(key)
            if cur is None or new_s > cur[0]:
                edges[key] = (new_s, edge_tier)

        if include_one_way:
            for other in (only_following | only_followers):
                key = tuple(sorted([seed, other]))
                new_s = TIER_STRENGTH[one_way_tier]
                if key in edges:
                    continue  # mutual / stronger evidence wins
                edges[key] = (new_s, one_way_tier)

        print(
            f"info: seed @{seed}: following={len(following_set)} "
            f"followers={len(followers_set)} mutual={len(mutual)} "
            f"emitted_edges={len(mutual) + (len(only_following | only_followers) if include_one_way else 0)}",
            file=sys.stderr,
        )

    os.makedirs(out_dir, exist_ok=True)

    people_path = os.path.join(out_dir, "people.csv")
    edges_path = os.path.join(out_dir, "edges.csv")
    identities_path = os.path.join(out_dir, "identities.csv")

    # Sort handles for deterministic output
    sorted_handles = sorted(all_handles.items())

    with open(people_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "name"])
        for h, name in sorted_handles:
            w.writerow([person_id(h), name or h])

    with open(edges_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["from", "to", "strength", "tier"])
        for (a, b), (s, t) in sorted(edges.items()):
            w.writerow([person_id(a), person_id(b), s, t])

    with open(identities_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["person_id", "platform", "handle"])
        for h, _ in sorted_handles:
            w.writerow([person_id(h), "twitter", f"@{h}"])

    return {
        "people": people_path,
        "edges": edges_path,
        "identities": identities_path,
        "people_count": len(sorted_handles),
        "edge_count": len(edges),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Convert Twitter follower/following CSVs into warm_intro CSVs.",
        epilog=(
            "Each --seed must be paired with one --following and one --followers, "
            "in the same order. Repeat the trio for additional seed users."
        ),
    )
    parser.add_argument(
        "--seed", action="append", required=True,
        help="Twitter handle of the seed account (e.g. @alice). Repeatable.",
    )
    parser.add_argument(
        "--following", action="append", required=True,
        help="CSV of accounts the seed follows. Repeat once per --seed.",
    )
    parser.add_argument(
        "--followers", action="append", required=True,
        help="CSV of accounts that follow the seed. Repeat once per --seed.",
    )
    parser.add_argument(
        "--out-dir", default=".",
        help="Directory where people.csv / edges.csv / identities.csv are written.",
    )
    parser.add_argument(
        "--include-one-way", action="store_true",
        help="Also emit edges for one-way follows (default: mutual-only).",
    )
    parser.add_argument(
        "--tier", default="platform_similarity",
        choices=sorted(TIER_STRENGTH.keys()),
        help="Tier label for mutual-follow edges (default: platform_similarity, "
             "since a Twitter follow is weaker evidence than a real-world tie).",
    )
    args = parser.parse_args(argv)

    if not (len(args.seed) == len(args.following) == len(args.followers)):
        parser.error(
            "--seed, --following, --followers must each appear the same number "
            "of times (one per seed user)."
        )

    seeds = list(zip(args.seed, args.following, args.followers))
    try:
        result = ingest(
            seeds,
            out_dir=args.out_dir,
            include_one_way=args.include_one_way,
            edge_tier=args.tier,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    print(
        f"\nWrote {result['people_count']} people and {result['edge_count']} "
        f"edges to {args.out_dir}/"
    )
    print(f"  people.csv     -> {result['people']}")
    print(f"  edges.csv      -> {result['edges']}")
    print(f"  identities.csv -> {result['identities']}")
    print(
        "\nNext step:\n"
        f"  python warm_intro.py --people {result['people']} "
        f"--edges {result['edges']} \\\n"
        f"      --identities {result['identities']} "
        f"--entry tw_<seed_handle> --target tw_<other_handle>"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
