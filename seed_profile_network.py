"""Generate a synthetic 3-degree social network centered on a user profile.

Reads `user_profile.json` (or accepts a profile dict) and emits three CSVs
consumed by the existing warm_intro engine:

    people.csv       (id column)
    edges.csv        (from,to,strength,tier) — interaction-aware strengths
    identities.csv   (person_id,twitter,farcaster,linkedin,debank)

Edge strengths follow the spec:
    Twitter   mutual + interactions = 10 (direct)    mutual only = 8 (mutual)
    LinkedIn  conn   + recommendation = 10 (direct)  conn   only = 7 (mutual)
    Farcaster mutual + interactions = 10 (direct)    mutual only = 8 (mutual)
    DeBank    wallet transactions   = 10 (direct)    follow only = 5 (platform_similarity)

The profile person is always given the fixed id `me` so the pathfinder has
a deterministic entry point.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta


PROFILE_ID = "me"

# Per-platform interaction columns written into identities.csv.
INTERACTION_COLUMNS = (
    "twitter_likes", "twitter_comments", "twitter_reposts",
    "farcaster_recasts", "farcaster_replies",
    "linkedin_recommendations", "linkedin_endorsements",
    "debank_transactions",
)

# Scoring bands from the spec, applied per column, summed, capped at 15.
#   1-5 → +1    6-20 → +3    21-50 → +5    51+ → +7
INTERACTION_SCORE_CAP = 15


def _score_for_count(n: int) -> int:
    if n <= 0:
        return 0
    if n <= 5:
        return 1
    if n <= 20:
        return 3
    if n <= 50:
        return 5
    return 7


def interaction_score(counts: dict[str, int]) -> int:
    total = sum(_score_for_count(counts.get(c, 0)) for c in INTERACTION_COLUMNS)
    return min(total, INTERACTION_SCORE_CAP)


def _random_interaction_counts(
    ident: dict, rng: random.Random
) -> dict[str, int]:
    """Produce realistic fake counts for every interaction column the
    person has a handle for. If a person has no Twitter handle, their
    `twitter_*` counts all stay 0 (can't interact on a platform you're
    not on)."""
    counts = {c: 0 for c in INTERACTION_COLUMNS}
    # Realistic lurker-heavy distribution: most people have 0-few
    # interactions on most platforms; a minority are highly active.
    if ident.get("twitter"):
        counts["twitter_likes"]    = rng.choices([0, 3, 12, 35, 75], [0.55, 0.25, 0.12, 0.06, 0.02])[0]
        counts["twitter_comments"] = rng.choices([0, 2, 8, 25, 60],  [0.6, 0.22, 0.12, 0.05, 0.01])[0]
        counts["twitter_reposts"]  = rng.choices([0, 2, 7, 22],      [0.65, 0.22, 0.1, 0.03])[0]
    if ident.get("farcaster"):
        counts["farcaster_recasts"] = rng.choices([0, 2, 8, 25], [0.65, 0.22, 0.1, 0.03])[0]
        counts["farcaster_replies"] = rng.choices([0, 3, 10, 30, 55], [0.6, 0.22, 0.12, 0.05, 0.01])[0]
    if ident.get("linkedin"):
        counts["linkedin_recommendations"] = rng.choices([0, 1, 3, 6], [0.78, 0.15, 0.05, 0.02])[0]
        counts["linkedin_endorsements"]    = rng.choices([0, 2, 8, 22, 60], [0.6, 0.22, 0.12, 0.05, 0.01])[0]
    if ident.get("debank"):
        counts["debank_transactions"] = rng.choices([0, 1, 5, 15, 40, 80], [0.55, 0.22, 0.12, 0.07, 0.03, 0.01])[0]
    return counts


def _random_last_interaction(rng: random.Random) -> str:
    """Random date within the last 90 days, formatted YYYY-MM-DD."""
    days_ago = rng.randint(0, 90)
    return (datetime.now() - timedelta(days=days_ago)).date().isoformat()

# Per-platform weights: tier -> strength. Picked per edge from the
# platforms shared by both endpoints.
EDGE_WEIGHTS: dict[str, dict[str, int]] = {
    "twitter":   {"direct": 10, "mutual": 8},
    "linkedin":  {"direct": 10, "mutual": 7},
    "farcaster": {"direct": 10, "mutual": 8},
    "debank":    {"direct": 10, "platform_similarity": 5},
}

FIRST_NAMES = [
    "Alice", "Ben", "Chen", "Diana", "Esme", "Finn", "Gauri", "Hugo", "Ishaan",
    "Jaspar", "Kenji", "Lara", "Milan", "Nora", "Omar", "Pascal", "Quinn",
    "Rosa", "Sana", "Tariq", "Uriel", "Vera", "Willa", "Ximena", "Yuki",
    "Zara", "Ava", "Bruno", "Carlo", "Dmitri", "Elena", "Farah", "Giulia",
    "Hassan", "Imani", "Jules", "Kira", "Leon", "Maya", "Niko", "Orla",
    "Priya", "Raj", "Sofia", "Theo", "Uma", "Vik", "Wyatt", "Xiu", "Yara",
    "Zane",
]
LAST_NAMES = [
    "Okafor", "Vargas", "Moreau", "Nakamura", "Costa", "Johansson", "Rashid",
    "Dutta", "Silva", "Beck", "Hakim", "Wang", "Tanaka", "Lindqvist",
    "Erikson", "Gupta", "Yamada", "Zhou", "Abara", "Ueda", "Kim",
]


def _rand_suffix(rng: random.Random) -> str:
    n = rng.randint(0, 3)
    return "".join(rng.choice("0123456789") for _ in range(n))


def _rand_wallet(rng: random.Random) -> str:
    return "0x" + "".join(rng.choice("0123456789abcdef") for _ in range(40))


def _build_identity(pid: str, name: str, rng: random.Random) -> dict:
    """Every synthetic person gets all four platforms so the path finder
    can find inter-person edges regardless of which platform we roll for
    a given edge. This maximizes demo-friendly connectivity."""
    base = name.lower().replace(" ", "")
    suffix = _rand_suffix(rng)
    return {
        "person_id": pid,
        "twitter": f"@{base}{suffix}",
        "farcaster": f"{base}{suffix}",
        "linkedin": name.lower().replace(" ", "-"),
        "debank": _rand_wallet(rng),
    }


def _platforms_with_handle(ident: dict) -> list[str]:
    return [p for p in ("twitter", "farcaster", "linkedin", "debank") if ident.get(p)]


def seed_for_profile(
    profile: dict,
    out_dir: str = ".",
    *,
    first_degree: int = 12,
    second_degree: int = 25,
    third_degree: int = 18,
    seed: int = 42,
) -> dict:
    """Generate and write the synthetic network. Returns output paths + counts."""
    rng = random.Random(seed)
    os.makedirs(out_dir, exist_ok=True)

    # ---- Build people + identities -----------------------------------
    me_identity = {
        "person_id": PROFILE_ID,
        "twitter": (profile.get("twitter") or "").strip(),
        "farcaster": (profile.get("farcaster") or "").strip(),
        "linkedin": (profile.get("linkedin") or "").strip(),
        "debank": (profile.get("debank") or "").strip(),
    }
    identities: list[dict] = [me_identity]
    people_ids: list[str] = [PROFILE_ID]

    used_names: set[str] = set()

    def gen_name() -> str:
        for _ in range(500):
            n = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
            if n not in used_names:
                used_names.add(n)
                return n
        return f"Person {len(used_names)}"

    def alloc(degree_prefix: str, count: int) -> list[str]:
        ids = []
        for i in range(count):
            name = gen_name()
            first = name.split()[0].lower()
            pid = f"{degree_prefix}_{i+1:02d}_{first}"
            ids.append(pid)
            people_ids.append(pid)
            identities.append(_build_identity(pid, name, rng))
        return ids

    first = alloc("p1", first_degree)
    second = alloc("p2", second_degree)
    third = alloc("p3", third_degree)

    ident_by_id = {i["person_id"]: i for i in identities}

    # ---- Build edges --------------------------------------------------
    edges: dict[tuple[str, str], tuple[int, str]] = {}

    def _add(a: str, b: str, strength: int, tier: str) -> None:
        if a == b:
            return
        k = (a, b) if a <= b else (b, a)
        prev = edges.get(k)
        if prev is None or strength > prev[0]:
            edges[k] = (strength, tier)

    def connect(a: str, b: str, intensity: str) -> None:
        """`intensity` is 'direct' (interactions/tx) or 'mutual' (follow only)."""
        shared = [
            p for p in _platforms_with_handle(ident_by_id[a])
            if p in _platforms_with_handle(ident_by_id[b])
        ]
        if not shared:
            return
        platform = rng.choice(shared)
        weights = EDGE_WEIGHTS[platform]
        if intensity == "direct" and "direct" in weights:
            tier = "direct"
        else:
            tier = next(t for t in weights if t != "direct")
        _add(a, b, weights[tier], tier)

    # me -> each first-degree contact (~40% direct / strong interactions)
    for p in first:
        connect(PROFILE_ID, p, "direct" if rng.random() < 0.4 else "mutual")

    # each first-degree -> 2-4 second-degree contacts
    for p in first:
        targets = rng.sample(second, k=min(rng.randint(2, 4), len(second)))
        for t in targets:
            connect(p, t, "direct" if rng.random() < 0.35 else "mutual")

    # each second-degree -> 1-3 third-degree contacts
    for p in second:
        targets = rng.sample(third, k=min(rng.randint(1, 3), len(third)))
        for t in targets:
            connect(p, t, "direct" if rng.random() < 0.3 else "mutual")

    # Cross-links within each degree so the graph has alternate paths
    for _ in range(max(first_degree // 2, 1)):
        a, b = rng.sample(first, 2)
        connect(a, b, "direct" if rng.random() < 0.4 else "mutual")
    for _ in range(max(second_degree // 2, 1)):
        a, b = rng.sample(second, 2)
        connect(a, b, "direct" if rng.random() < 0.3 else "mutual")
    for _ in range(max(third_degree // 3, 1)):
        a, b = rng.sample(third, 2)
        connect(a, b, "mutual")

    # A few shortcut edges from me -> second/third so some paths are
    # short while others are longer — lets the 5-ranked view be meaningful.
    for t in rng.sample(second, k=min(2, len(second))):
        connect(PROFILE_ID, t, "mutual")

    # ---- Interaction counts + score per person -----------------------
    # Generated AFTER people+edges so we can use the interaction_score
    # to boost each edge's base strength (capped at 15).
    interactions_by_pid: dict[str, dict[str, int]] = {}
    score_by_pid: dict[str, int] = {}
    last_by_pid: dict[str, str] = {}
    for ident in identities:
        pid = ident["person_id"]
        counts = _random_interaction_counts(ident, rng)
        interactions_by_pid[pid] = counts
        score_by_pid[pid] = interaction_score(counts)
        last_by_pid[pid] = _random_last_interaction(rng) if any(counts.values()) else ""

    # ---- Boost edge strengths with the larger endpoint's score -------
    # Example from spec: mutual (strength 8) + interaction_score (5) = 13.
    boosted_edges: dict[tuple[str, str], tuple[int, str]] = {}
    for (a, b), (base_strength, tier) in edges.items():
        boost = max(score_by_pid.get(a, 0), score_by_pid.get(b, 0))
        final = min(int(base_strength) + int(boost), 15)
        boosted_edges[(a, b)] = (final, tier)

    # ---- Write CSVs ---------------------------------------------------
    people_csv = os.path.join(out_dir, "people.csv")
    edges_csv = os.path.join(out_dir, "edges.csv")
    identities_csv = os.path.join(out_dir, "identities.csv")

    with open(people_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id"])
        for pid in people_ids:
            w.writerow([pid])

    with open(edges_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["from", "to", "strength", "tier"])
        for (a, b), (s, t) in sorted(boosted_edges.items()):
            w.writerow([a, b, s, t])

    # identities.csv now carries per-platform interaction columns,
    # plus person-level interaction_score and last_interaction.
    identities_header = [
        "person_id", "twitter", "farcaster", "linkedin", "debank",
        *INTERACTION_COLUMNS,
        "last_interaction", "interaction_score",
    ]
    with open(identities_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(identities_header)
        for ident in identities:
            pid = ident["person_id"]
            counts = interactions_by_pid[pid]
            w.writerow([
                pid, ident["twitter"], ident["farcaster"],
                ident["linkedin"], ident["debank"],
                *(counts[c] for c in INTERACTION_COLUMNS),
                last_by_pid[pid],
                score_by_pid[pid],
            ])

    return {
        "people": people_csv,
        "edges": edges_csv,
        "identities": identities_csv,
        "people_count": len(people_ids),
        "edge_count": len(boosted_edges),
    }


def load_profile(path: str = "user_profile.json") -> dict | None:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_profile(profile: dict, path: str = "user_profile.json") -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(profile, f, indent=2)


def default_profile() -> dict:
    return {
        "name": "You",
        "twitter": "@you",
        "farcaster": "you",
        "linkedin": "you",
        "debank": "0x" + "0" * 40,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate a synthetic network centered on user_profile.json."
    )
    parser.add_argument("--profile", default="user_profile.json")
    parser.add_argument("--out-dir", default=".")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    prof = load_profile(args.profile) or default_profile()
    result = seed_for_profile(prof, out_dir=args.out_dir, seed=args.seed)
    print(
        f"Wrote {result['people_count']} people, {result['edge_count']} edges "
        f"to {args.out_dir}/"
    )
    print(f"  people.csv     -> {result['people']}")
    print(f"  edges.csv      -> {result['edges']}")
    print(f"  identities.csv -> {result['identities']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
