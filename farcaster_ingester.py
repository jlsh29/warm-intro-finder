"""Farcaster follower/following + channel CSV -> warm_intro CSVs.

Usage
-----
    python farcaster_ingester.py \
        --seed 1001 --following alice_following.csv --followers alice_followers.csv \
        --seed 1002 --following bob_following.csv   --followers bob_followers.csv \
        --channels channels.csv \
        --out-dir ./farcaster_data

    # Then route through the existing engine:
    python warm_intro.py \
        --people farcaster_data/people.csv \
        --edges  farcaster_data/edges.csv \
        --identities farcaster_data/identities.csv \
        --entry fc_1001 --target fc_1003 --explain

Input format
------------
Every input CSV identifies people by their **FID** (numeric Farcaster
ID) - the permanent canonical identifier. Auto-detected aliases:
  fid:           fid | farcaster_id | user_fid | id
  username:      username | handle | fname
  display name:  display_name | name | display
  channel:       channel | channel_name | parent_url

following.csv / followers.csv: rows describe accounts the seed follows
or is followed by. Must include FID; username and display name optional.

channels.csv (optional): rows of (fid, channel) describing memberships.
Each channel becomes a `shared_org` clique edge between every pair of
its members.

Direction handling
------------------
Farcaster follows are directed. Warm intros need bidirectional
acquaintance, so by default this script emits an edge between a seed
and another FID ONLY when the follow is mutual (FID appears in both
seed's following AND followers files). Pass `--include-one-way` to
also emit one-way edges at the lower platform_similarity tier.

Output
------
Three CSVs in `--out-dir`:
  people.csv      (id, name)
  edges.csv       (from, to, strength, tier)
  identities.csv  (person_id, platform, handle)

Person ids are `fc_<fid>` - based on the immutable Farcaster ID, not
the mutable @username. Identity handles in identities.csv are the
@username so cross-platform humans can read them. The ingester also
captures the username in identities.csv so future identity-merging
can use it as evidence.

Safety
------
This script does not contact the Farcaster network or fabricate
edges. It only emits what is present in the input CSVs. Channel
co-membership produces shared_org edges only when explicit channel
membership is in the input data.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict
from itertools import combinations


FID_COLS = ("fid", "farcaster_id", "user_fid", "id")
USERNAME_COLS = ("username", "handle", "fname")
NAME_COLS = ("display_name", "name", "display")
CHANNEL_COLS = ("channel", "channel_name", "parent_url")

TIER_STRENGTH: dict[str, int] = {
    "direct": 10,
    "mutual": 8,
    "shared_org": 5,
    "platform_similarity": 2,
}


def detect_col(headers: list[str], candidates: tuple[str, ...]) -> str | None:
    """Find a header matching any candidate, treating space and underscore as equivalent."""
    norm = {h.lower().strip().replace(" ", "_"): h for h in headers}
    for cand in candidates:
        if cand in norm:
            return norm[cand]
    return None


def normalize_fid(s: str) -> str:
    """Return canonical FID string. Strips whitespace, drops non-numeric input."""
    s = (s or "").strip()
    if not s:
        return ""
    # Allow FIDs prefixed with `fid:` or similar; keep only digits.
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits


def normalize_username(s: str) -> str:
    """Strip whitespace + leading `@`, lowercase. Empty input returns ''."""
    s = (s or "").strip().lstrip("@")
    return s.lower() if s else ""


def person_id(fid: str) -> str:
    """Namespace a Farcaster numeric FID into a person id (e.g. `fc_1001`)."""
    return f"fc_{fid}"


def read_accounts_csv(path: str) -> dict[str, dict]:
    """Parse a follower/following CSV. Returns {fid: {username, name}}."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"input file not found: {path}")
    accounts: dict[str, dict] = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"{path}: empty CSV")
        fid_col = detect_col(reader.fieldnames, FID_COLS)
        if not fid_col:
            raise ValueError(
                f"{path}: no FID column. Looked for one of {FID_COLS}; "
                f"got {reader.fieldnames}"
            )
        username_col = detect_col(reader.fieldnames, USERNAME_COLS)
        name_col = detect_col(reader.fieldnames, NAME_COLS)
        for row in reader:
            fid = normalize_fid(row.get(fid_col, ""))
            if not fid:
                continue
            username = normalize_username(row.get(username_col, "") if username_col else "")
            name = (row.get(name_col, "") or "").strip() if name_col else ""
            existing = accounts.get(fid, {})
            # Keep the longest non-empty value seen per field.
            accounts[fid] = {
                "username": username if (not existing.get("username") or len(username) > len(existing.get("username", ""))) else existing.get("username", ""),
                "name": name if (not existing.get("name") or len(name) > len(existing.get("name", ""))) else existing.get("name", ""),
            }
    return accounts


def read_channels_csv(path: str) -> dict[str, set[str]]:
    """Parse a channels CSV. Returns {channel_name: {fid, ...}}."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"channels file not found: {path}")
    channels: dict[str, set[str]] = defaultdict(set)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"{path}: empty CSV")
        fid_col = detect_col(reader.fieldnames, FID_COLS)
        channel_col = detect_col(reader.fieldnames, CHANNEL_COLS)
        if not fid_col or not channel_col:
            raise ValueError(
                f"{path}: needs both an FID column and a channel column. "
                f"Got: {reader.fieldnames}"
            )
        for row in reader:
            fid = normalize_fid(row.get(fid_col, ""))
            channel = (row.get(channel_col, "") or "").strip()
            if not fid or not channel:
                continue
            channels[channel].add(fid)
    return dict(channels)


def ingest(
    seeds: list[tuple[str, str, str]],  # (seed_fid, following_path, followers_path)
    out_dir: str,
    include_one_way: bool = False,
    edge_tier: str = "mutual",
    one_way_tier: str = "platform_similarity",
    channels_path: str | None = None,
) -> dict:
    """Convert per-seed Farcaster follow CSVs (+ optional channels) into engine CSVs.

    Mutual-follow logic mirrors the Twitter ingester. If `channels_path`
    is given, channel co-membership additionally produces shared_org
    edges (strength 5) between every pair of channel members; mutual
    follow evidence wins on dedup so a stronger edge is never downgraded.
    Returns the three output paths plus counts.
    """
    if edge_tier not in TIER_STRENGTH:
        raise ValueError(f"unknown tier {edge_tier!r}")
    if one_way_tier not in TIER_STRENGTH:
        raise ValueError(f"unknown one-way tier {one_way_tier!r}")

    # fid -> {username, name}
    all_people: dict[str, dict] = {}

    # sorted-pair -> (strength, tier, attributes)
    edges: dict[tuple[str, str], tuple[int, str, dict]] = {}

    def upsert_person(fid: str, username: str = "", name: str = "") -> None:
        existing = all_people.get(fid, {})
        all_people[fid] = {
            "username": username if (not existing.get("username") or len(username) > len(existing.get("username", ""))) else existing.get("username", ""),
            "name": name if (not existing.get("name") or len(name) > len(existing.get("name", ""))) else existing.get("name", ""),
        }

    def upsert_edge(a: str, b: str, strength: int, tier: str, attrs: dict | None = None) -> None:
        key = tuple(sorted([a, b]))
        cur = edges.get(key)
        if cur is None or strength > cur[0]:
            edges[key] = (strength, tier, attrs or {})

    seen_seeds: set[str] = set()
    for seed_raw, following_path, followers_path in seeds:
        seed_fid = normalize_fid(seed_raw)
        if not seed_fid:
            raise ValueError(f"empty or non-numeric --seed value: {seed_raw!r}")
        if seed_fid in seen_seeds:
            print(f"warning: seed FID {seed_fid!r} listed twice; ignoring duplicate",
                  file=sys.stderr)
            continue
        seen_seeds.add(seed_fid)

        following = read_accounts_csv(following_path)
        followers = read_accounts_csv(followers_path)
        following.pop(seed_fid, None)
        followers.pop(seed_fid, None)

        # Register the seed as a person (no metadata yet).
        upsert_person(seed_fid)

        for fid, meta in following.items():
            upsert_person(fid, meta.get("username", ""), meta.get("name", ""))
        for fid, meta in followers.items():
            upsert_person(fid, meta.get("username", ""), meta.get("name", ""))

        following_set = set(following)
        followers_set = set(followers)
        mutual = following_set & followers_set
        only_following = following_set - followers_set
        only_followers = followers_set - following_set

        for other in mutual:
            upsert_edge(seed_fid, other, TIER_STRENGTH[edge_tier], edge_tier)

        if include_one_way:
            for other in (only_following | only_followers):
                # Don't downgrade existing mutual evidence
                key = tuple(sorted([seed_fid, other]))
                if key in edges and edges[key][1] == edge_tier:
                    continue
                upsert_edge(seed_fid, other, TIER_STRENGTH[one_way_tier], one_way_tier)

        emitted = len(mutual) + (
            len(only_following | only_followers) if include_one_way else 0
        )
        print(
            f"info: seed fid={seed_fid}: following={len(following_set)} "
            f"followers={len(followers_set)} mutual={len(mutual)} "
            f"emitted_edges={emitted}",
            file=sys.stderr,
        )

    # Channel-derived shared_org edges (after follow processing so mutual wins)
    if channels_path:
        channels = read_channels_csv(channels_path)
        channel_edges = 0
        for channel_name, members in channels.items():
            if len(members) < 2:
                continue
            # Make sure all channel members appear in people.csv even if they
            # weren't in any follow file.
            for fid in members:
                upsert_person(fid)
            for a, b in combinations(sorted(members), 2):
                key = tuple(sorted([a, b]))
                # Skip if a stronger follow-derived edge already exists.
                cur = edges.get(key)
                cur_strength = cur[0] if cur else 0
                if cur_strength >= TIER_STRENGTH["shared_org"]:
                    continue
                upsert_edge(a, b, TIER_STRENGTH["shared_org"], "shared_org",
                            {"channel": channel_name})
                channel_edges += 1
        print(
            f"info: channels: {len(channels)} channel(s), "
            f"{channel_edges} shared_org edge(s) added",
            file=sys.stderr,
        )

    os.makedirs(out_dir, exist_ok=True)
    people_path = os.path.join(out_dir, "people.csv")
    edges_path = os.path.join(out_dir, "edges.csv")
    identities_path = os.path.join(out_dir, "identities.csv")

    sorted_people = sorted(all_people.items(), key=lambda kv: int(kv[0]))

    with open(people_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        # Id-only schema: fc_<fid> IS the identity. Username + display
        # name are surfaced through identities.csv rather than baked
        # into people.csv.
        w.writerow(["id"])
        for fid, _meta in sorted_people:
            w.writerow([person_id(fid)])

    with open(edges_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["from", "to", "strength", "tier"])
        for (a, b), (s, t, _attrs) in sorted(edges.items()):
            w.writerow([person_id(a), person_id(b), s, t])

    # Wide-format identities.csv.
    with open(identities_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["person_id", "twitter", "farcaster", "linkedin", "debank"])
        for fid, meta in sorted_people:
            handle = meta["username"] if meta.get("username") else f"fid:{fid}"
            w.writerow([person_id(fid), "", handle, "", ""])

    return {
        "people": people_path,
        "edges": edges_path,
        "identities": identities_path,
        "people_count": len(sorted_people),
        "edge_count": len(edges),
    }


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Parse argv, call ingest(), print result. Returns exit code."""
    parser = argparse.ArgumentParser(
        description="Convert Farcaster follower/following + channel CSVs into warm_intro CSVs.",
        epilog=(
            "Each --seed must be paired with one --following and one --followers, "
            "in the same order. Repeat the trio for additional seeds. "
            "--channels is a single CSV (one channel/fid per row) and is optional."
        ),
    )
    parser.add_argument(
        "--seed", action="append", required=True,
        help="Numeric FID of the seed account (e.g. 1001). Repeatable.",
    )
    parser.add_argument(
        "--following", action="append", required=True,
        help="CSV of accounts the seed follows (must contain FID column).",
    )
    parser.add_argument(
        "--followers", action="append", required=True,
        help="CSV of accounts following the seed (must contain FID column).",
    )
    parser.add_argument(
        "--channels", default=None,
        help="Optional CSV of channel memberships (cols: fid, channel). "
             "Channel co-membership emits shared_org-tier edges between members.",
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
        "--tier", default="mutual",
        choices=sorted(TIER_STRENGTH.keys()),
        help="Tier label for mutual-follow edges (default: mutual; Farcaster "
             "follows are more deliberate than Twitter's, so a stronger label "
             "than platform_similarity is reasonable).",
    )
    args = parser.parse_args(argv)

    n = len(args.seed)
    if not (n == len(args.following) == len(args.followers)):
        parser.error(
            "--seed, --following, --followers must each appear the same number "
            "of times (one per seed FID)."
        )

    seeds = list(zip(args.seed, args.following, args.followers))
    try:
        result = ingest(
            seeds,
            out_dir=args.out_dir,
            include_one_way=args.include_one_way,
            edge_tier=args.tier,
            channels_path=args.channels,
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
        f"--entry fc_<seed_fid> --target fc_<other_fid> --explain"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
