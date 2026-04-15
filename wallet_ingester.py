"""Blockchain wallet interactions CSV -> warm_intro people.csv / edges.csv / identities.csv.

Usage
-----
    python wallet_ingester.py \
        --interactions interactions.csv \
        --mapping wallet_mapping.csv \
        --out-dir ./wallet_data \
        --mutual-threshold 3

    # Then route through the existing engine:
    python warm_intro.py \
        --people wallet_data/people.csv \
        --edges  wallet_data/edges.csv \
        --identities wallet_data/identities.csv \
        --entry alice --target bob --explain

Input format
------------
interactions.csv (required): pairwise wallet activity. Auto-detected
column aliases:
  from:    from | from_wallet | from_address | sender | source
  to:      to   | to_wallet   | to_address   | receiver | recipient | target
  count:   count | tx_count | interaction_count | n
  type:    type | interaction_type | tx_type   (optional)

Each row aggregates how many interactions (transfers, calls, etc.)
flowed from one wallet to another. Direction is preserved internally
to compute reciprocity but the emitted edge is undirected.

mapping.csv (optional): wallet -> person mapping. Required columns:
  person_id, wallet
  optional: person_name
Multiple wallets can map to the same person_id (cross-wallet
ownership). Unmapped wallets become their own person with id
`wal_<address>` so the data isn't lost; the identity layer can
collapse them later when more evidence arrives.

Edge tiers
----------
A pair's tier depends on total bidirectional interaction count:
  count >= mutual_threshold (default 3): tier=mutual,            strength=8
  count >= 1:                            tier=platform_similarity, strength=2

Override `--tier` to force a specific tier on every edge instead.

Output
------
Three CSVs in `--out-dir`:
  people.csv      (id, name)
  edges.csv       (from, to, strength, tier)
  identities.csv  (person_id, platform, handle)

Person ids:
  - For mapped wallets, the `person_id` from the mapping is used as-is
    (so it can collide cleanly with ids from other ingesters when you
    pre-stitch identities).
  - For unmapped wallets, `wal_<lowercase_address>` is generated.

Identity rows attach every input wallet to its person with platform="wallet".

Safety
------
This script does not contact any RPC, indexer, or chain explorer. It
only emits edges that are present in the input CSVs. Wallet -> person
linkage is taken from the mapping you provide, never inferred. The
ingester does not heuristically merge wallets based on activity patterns.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict


FROM_COLS = ("from", "from_wallet", "from_address", "sender", "source")
TO_COLS = ("to", "to_wallet", "to_address", "receiver", "recipient", "target")
COUNT_COLS = ("count", "tx_count", "interaction_count", "n")
TYPE_COLS = ("type", "interaction_type", "tx_type")

MAP_PERSON_COLS = ("person_id", "person", "id")
MAP_WALLET_COLS = ("wallet", "wallet_address", "address")
MAP_NAME_COLS = ("person_name", "name", "display_name")

TIER_STRENGTH: dict[str, int] = {
    "direct": 10,
    "mutual": 8,
    "shared_org": 5,
    "platform_similarity": 2,
}


def detect_col(headers: list[str], candidates: tuple[str, ...]) -> str | None:
    norm = {h.lower().strip().replace(" ", "_"): h for h in headers}
    for cand in candidates:
        if cand in norm:
            return norm[cand]
    return None


def normalize_wallet(s: str) -> str:
    """Lowercase EVM addresses; pass through other formats unchanged.

    Real addresses encode a checksum in their case but for graph keying
    we want case-insensitivity. Non-EVM (e.g. Solana base58, Cosmos
    bech32) are left as-is.
    """
    s = (s or "").strip()
    if not s:
        return ""
    if s.startswith("0x") and len(s) == 42 and all(c in "0123456789abcdefABCDEF" for c in s[2:]):
        return s.lower()
    return s


def short_addr(addr: str) -> str:
    if len(addr) > 14:
        return f"{addr[:6]}...{addr[-4:]}"
    return addr


def read_mapping_csv(path: str) -> tuple[dict[str, str], dict[str, str]]:
    """Parse wallet -> person mapping. Returns (wallet -> person_id, person_id -> name)."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"mapping file not found: {path}")
    wallet_to_person: dict[str, str] = {}
    person_to_name: dict[str, str] = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"{path}: empty CSV")
        person_col = detect_col(reader.fieldnames, MAP_PERSON_COLS)
        wallet_col = detect_col(reader.fieldnames, MAP_WALLET_COLS)
        name_col = detect_col(reader.fieldnames, MAP_NAME_COLS)
        if not person_col or not wallet_col:
            raise ValueError(
                f"{path}: needs both a person id column and a wallet column. "
                f"Got: {reader.fieldnames}"
            )
        for row in reader:
            pid = (row.get(person_col, "") or "").strip()
            wallet = normalize_wallet(row.get(wallet_col, ""))
            if not pid or not wallet:
                continue
            if wallet in wallet_to_person and wallet_to_person[wallet] != pid:
                print(
                    f"warning: wallet {wallet!r} mapped to both "
                    f"{wallet_to_person[wallet]!r} and {pid!r}; "
                    f"keeping first ({wallet_to_person[wallet]!r}). "
                    f"This is likely a data error - reconcile in mapping.csv.",
                    file=sys.stderr,
                )
                continue
            wallet_to_person[wallet] = pid
            if name_col:
                name = (row.get(name_col, "") or "").strip()
                if name and (pid not in person_to_name or len(name) > len(person_to_name[pid])):
                    person_to_name[pid] = name
    return wallet_to_person, person_to_name


def read_interactions_csv(path: str) -> list[tuple[str, str, int, str]]:
    """Parse interaction rows. Returns list of (from_wallet, to_wallet, count, type)."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"interactions file not found: {path}")
    rows: list[tuple[str, str, int, str]] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"{path}: empty CSV")
        from_col = detect_col(reader.fieldnames, FROM_COLS)
        to_col = detect_col(reader.fieldnames, TO_COLS)
        count_col = detect_col(reader.fieldnames, COUNT_COLS)
        type_col = detect_col(reader.fieldnames, TYPE_COLS)
        if not from_col or not to_col:
            raise ValueError(
                f"{path}: needs both a `from` and `to` column. "
                f"Got: {reader.fieldnames}"
            )
        for row in reader:
            a = normalize_wallet(row.get(from_col, ""))
            b = normalize_wallet(row.get(to_col, ""))
            if not a or not b or a == b:
                continue
            count = 1
            if count_col:
                raw = (row.get(count_col, "") or "").strip()
                if raw:
                    try:
                        count = max(1, int(float(raw)))
                    except ValueError:
                        count = 1
            t = (row.get(type_col, "") or "").strip() if type_col else ""
            rows.append((a, b, count, t))
    return rows


def ingest(
    interactions_path: str,
    out_dir: str,
    mapping_path: str | None = None,
    mutual_threshold: int = 3,
    forced_tier: str | None = None,
) -> dict:
    if forced_tier is not None and forced_tier not in TIER_STRENGTH:
        raise ValueError(f"unknown tier {forced_tier!r}")

    wallet_to_person: dict[str, str] = {}
    person_to_name: dict[str, str] = {}
    if mapping_path:
        wallet_to_person, person_to_name = read_mapping_csv(mapping_path)

    interactions = read_interactions_csv(interactions_path)

    def person_for(wallet: str) -> str:
        if wallet in wallet_to_person:
            return wallet_to_person[wallet]
        return f"wal_{wallet}"

    # Aggregate undirected counts per person pair
    pair_count: dict[tuple[str, str], int] = defaultdict(int)
    # Track every wallet ever seen so the identities + people lists are complete
    seen_wallets: set[str] = set(wallet_to_person)
    for a, b, count, _t in interactions:
        seen_wallets.add(a)
        seen_wallets.add(b)
        pa, pb = person_for(a), person_for(b)
        if pa == pb:
            continue  # self-interaction across own wallets
        key = tuple(sorted([pa, pb]))
        pair_count[key] += count

    # People: union of mapped persons + wal_* persons for every unmapped wallet
    people: dict[str, str] = {}  # person_id -> display_name
    for pid, name in person_to_name.items():
        people[pid] = name or pid
    for w in seen_wallets:
        if w in wallet_to_person:
            pid = wallet_to_person[w]
            people.setdefault(pid, person_to_name.get(pid, pid))
        else:
            pid = f"wal_{w}"
            people.setdefault(pid, short_addr(w))

    # Edges
    edges: dict[tuple[str, str], tuple[int, str]] = {}
    for (a, b), n in pair_count.items():
        if forced_tier:
            tier = forced_tier
        elif n >= mutual_threshold:
            tier = "mutual"
        else:
            tier = "platform_similarity"
        edges[(a, b)] = (TIER_STRENGTH[tier], tier)

    os.makedirs(out_dir, exist_ok=True)
    people_path = os.path.join(out_dir, "people.csv")
    edges_path = os.path.join(out_dir, "edges.csv")
    identities_path = os.path.join(out_dir, "identities.csv")

    with open(people_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "name"])
        for pid in sorted(people):
            w.writerow([pid, people[pid]])

    with open(edges_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["from", "to", "strength", "tier"])
        for (a, b), (s, t) in sorted(edges.items()):
            w.writerow([a, b, s, t])

    with open(identities_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["person_id", "platform", "handle"])
        # Every wallet -> identity row, even those without explicit mapping.
        for wallet in sorted(seen_wallets):
            pid = person_for(wallet)
            w.writerow([pid, "wallet", wallet])

    print(
        f"info: interactions={len(interactions)} unique_wallets={len(seen_wallets)} "
        f"persons={len(people)} edges={len(edges)} "
        f"(mutual_threshold={mutual_threshold})",
        file=sys.stderr,
    )
    return {
        "people": people_path,
        "edges": edges_path,
        "identities": identities_path,
        "people_count": len(people),
        "edge_count": len(edges),
        "wallet_count": len(seen_wallets),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Convert wallet-interaction CSVs into warm_intro CSVs.",
        epilog=(
            "Pre-aggregate your interactions if possible (one row per wallet pair "
            "with a count column). The ingester sums counts across rows and "
            "across mapped wallets per person."
        ),
    )
    parser.add_argument(
        "--interactions", required=True,
        help="CSV of wallet-to-wallet interactions (cols: from, to, [count, type]).",
    )
    parser.add_argument(
        "--mapping", default=None,
        help="Optional CSV of wallet -> person mapping (cols: person_id, wallet, [person_name]).",
    )
    parser.add_argument(
        "--out-dir", default=".",
        help="Directory where people.csv / edges.csv / identities.csv are written.",
    )
    parser.add_argument(
        "--mutual-threshold", type=int, default=3,
        help="Combined interaction count >= this = mutual tier (8); below = platform_similarity (2).",
    )
    parser.add_argument(
        "--tier", default=None, choices=sorted(TIER_STRENGTH.keys()),
        help="Force every edge to use this tier (overrides --mutual-threshold).",
    )
    args = parser.parse_args(argv)

    try:
        result = ingest(
            interactions_path=args.interactions,
            out_dir=args.out_dir,
            mapping_path=args.mapping,
            mutual_threshold=args.mutual_threshold,
            forced_tier=args.tier,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    print(
        f"\nWrote {result['people_count']} people and {result['edge_count']} "
        f"edges to {args.out_dir}/ ({result['wallet_count']} unique wallets)"
    )
    print(f"  people.csv     -> {result['people']}")
    print(f"  edges.csv      -> {result['edges']}")
    print(f"  identities.csv -> {result['identities']}")
    print(
        "\nNext step:\n"
        f"  python warm_intro.py --people {result['people']} "
        f"--edges {result['edges']} \\\n"
        f"      --identities {result['identities']} "
        f"--entry <person_id_or_wal_addr> --target <other_person_id> --explain"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
