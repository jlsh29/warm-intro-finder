"""Generate a synthetic professional-network dataset.

Produces people.csv (100 rows) and edges.csv (exactly 200 undirected edges).

Realism model
-------------
People are distributed across 6 companies of varying sizes, each split into
2-4 teams. Edge generation has three tiers, reflecting how professional
networks actually cluster:

  1. Intra-team (strongest)   — coworkers on the same team know each other
  2. Intra-company cross-team — weaker but common (all-hands, projects)
  3. Cross-company            — ex-colleagues, conference contacts, alumni

Seeded with SEED=42 so the output is deterministic and re-runnable.
"""

from __future__ import annotations

import csv
import random
import re
from itertools import combinations

SEED = 42
N_PEOPLE = 100
N_EDGES = 200

FIRST_NAMES = [
    "Aaron", "Bella", "Chen", "Divya", "Elena", "Felix", "Gabriela", "Hassan",
    "Ishaan", "Julia", "Kenji", "Lara", "Mateo", "Nora", "Omar", "Priya",
    "Quentin", "Rosa", "Soren", "Tariq", "Ursula", "Viktor", "Willa", "Xiu",
    "Yosef", "Zara", "Anika", "Bruno", "Cassie", "Darius", "Esme", "Finn",
    "Giulia", "Hugo", "Imani", "Jaspar", "Kalani", "Lucia", "Milan", "Noa",
    "Orla", "Pascal", "Rafael", "Sana", "Tomoko", "Uriel", "Vera", "Wyatt",
    "Ximena", "Yuki",
]
LAST_NAMES = [
    "Abara", "Beck", "Costa", "Dutta", "Erikson", "Flores", "Gupta", "Hakim",
    "Ito", "Johansson", "Kapoor", "Lindqvist", "Moreau", "Nakamura", "Okafor",
    "Park", "Quinn", "Rashid", "Silva", "Tanaka", "Ueda", "Vargas", "Wang",
    "Xu", "Yamada", "Zhou",
]

COMPANIES = [
    ("Helix Labs",       22),
    ("Northwind Capital", 20),
    ("Ember Robotics",    18),
    ("Paloma Health",     16),
    ("Brightline AI",     14),
    ("Solstice Partners", 10),
]
assert sum(n for _, n in COMPANIES) == N_PEOPLE

ROLES = [
    "Engineer", "Senior Engineer", "Staff Engineer", "Engineering Manager",
    "Product Manager", "Designer", "Data Scientist", "Researcher",
    "Analyst", "Partner", "Principal", "Founder",
]


# Priority order for resolving a person's single canonical identity.
# Probabilities are independent per-platform - some persons end up with
# multiple platforms available, but only the highest-priority one becomes
# their canonical id.
PLATFORM_PROBABILITIES = [
    ("twitter", 0.70),
    ("farcaster", 0.60),
    ("linkedin", 0.50),
    ("wallet", 0.30),
]


def _slugify(part: str) -> str:
    """Normalize a name fragment to a handle-safe lowercase token."""
    return re.sub(r"[^a-z0-9]+", "", part.lower())


def _claim(used: set[str], base: str) -> str:
    """Return the first unused handle at or after `base`, suffixing a number if needed."""
    if base and base not in used:
        used.add(base)
        return base
    n = 2
    while f"{base}{n}" in used:
        n += 1
    claimed = f"{base}{n}"
    used.add(claimed)
    return claimed


def _fake_eth_address(rng: random.Random) -> str:
    """Generate a fake `0x` + 40-hex EVM-style address. Used only for wallet-only persons."""
    return "0x" + "".join(rng.choice("0123456789abcdef") for _ in range(40))


def generate_people(rng: random.Random) -> tuple[list[dict], dict[str, list[str]]]:
    """Generate people, each with at least one platform identity.

    Each person rolls 4 independent platform-presence coins. If all four
    miss, the person is SKIPPED entirely - they're not added to
    `people`, not added to `team_members`, and therefore never appear
    in the final people.csv, edges.csv, or identities.csv. No
    `unknown_user_*` rows are produced.

    For included persons the canonical `id` uses the highest-priority
    platform as its namespace prefix; the wide-format `handles` dict
    records ALL platform handles the person owns so identities.csv can
    expose every link in the Flask UI.

    Real first/last names are scratch values used only to build
    realistic-looking handles; they are never written to any CSV.
    Company and team stay on the in-memory dict because `generate_edges`
    uses them to weight tier-correct edges; they're also never written.
    """
    people: list[dict] = []
    team_members: dict[str, list[str]] = {}
    used = {p: set() for p, _ in PLATFORM_PROBABILITIES}

    for company, size in COMPANIES:
        n_teams = max(2, min(4, size // 6))
        team_sizes = [size // n_teams] * n_teams
        for i in range(size - sum(team_sizes)):
            team_sizes[i] += 1
        for t_idx, t_size in enumerate(team_sizes, start=1):
            team_key = f"{company} / Team {t_idx}"
            team_members[team_key] = []
            for _ in range(t_size):
                first = _slugify(rng.choice(FIRST_NAMES))
                last = _slugify(rng.choice(LAST_NAMES))

                present = {
                    p: rng.random() < prob for p, prob in PLATFORM_PROBABILITIES
                }
                # Skip entirely if no platform rolled in. Keeps the
                # graph clean of `unknown_user_*` entries.
                if not any(present.values()):
                    continue

                # Generate every handle the person has, per-platform
                # deduped against global sets so the same handle
                # doesn't appear twice anywhere in the system.
                handles: dict[str, str] = {}
                if present["twitter"]:
                    h = _claim(used["twitter"], f"{first}_{last}")
                    handles["twitter"] = f"@{h}"
                if present["farcaster"]:
                    h = _claim(used["farcaster"], first)
                    handles["farcaster"] = h
                if present["linkedin"]:
                    h = _claim(used["linkedin"], f"{first}-{last}")
                    handles["linkedin"] = h
                if present["wallet"]:
                    addr = _claim(used["wallet"], _fake_eth_address(rng))
                    handles["wallet"] = addr

                # Priority resolution for the namespaced id.
                if "twitter" in handles:
                    person_id = f"tw_{handles['twitter'].lstrip('@')}"
                    primary = "twitter"
                elif "farcaster" in handles:
                    person_id = f"fc_{handles['farcaster']}"
                    primary = "farcaster"
                elif "linkedin" in handles:
                    person_id = f"li_{handles['linkedin']}"
                    primary = "linkedin"
                else:  # wallet only
                    person_id = f"wal_{handles['wallet']}"
                    primary = "wallet"

                people.append(
                    {
                        "id": person_id,
                        "company": company,
                        "team": team_key,
                        "primary": primary,
                        "handles": handles,  # all filled platforms
                    }
                )
                team_members[team_key].append(person_id)
    return people, team_members


TIER_STRENGTH_RANGES = {
    "intra_team": (7, 10),
    "intra_company": (4, 6),
    "cross_company": (1, 3),
}


def generate_edges(
    rng: random.Random,
    people: list[dict],
    team_members: dict[str, list[str]],
) -> list[tuple[str, str, int]]:
    by_id = {p["id"]: p for p in people}
    edges: dict[tuple[str, str], int] = {}

    def roll(tier: str) -> int:
        lo, hi = TIER_STRENGTH_RANGES[tier]
        return rng.randint(lo, hi)

    def add(a: str, b: str, tier: str) -> bool:
        if a == b:
            return False
        edge = (a, b) if a < b else (b, a)
        if edge in edges:
            return False
        edges[edge] = roll(tier)
        return True

    # Budget: ~60% intra-team, ~15% intra-company, ~25% cross-company.
    # Realistic professional networks have bridges between organizations
    # (ex-colleagues, conferences, alumni), so we must leave room for them.
    tier1_budget = int(N_EDGES * 0.60)
    tier2_budget = int(N_EDGES * 0.15)

    # Tier 1 — intra-team: sample pairs within each team until budget filled.
    team_pairs: list[tuple[str, str]] = []
    for members in team_members.values():
        team_pairs.extend(combinations(members, 2))
    rng.shuffle(team_pairs)
    for a, b in team_pairs:
        if len(edges) >= tier1_budget:
            break
        add(a, b, "intra_team")

    # Tier 2 — intra-company cross-team: weaker ties within the same company.
    by_company: dict[str, list[str]] = {}
    for p in people:
        by_company.setdefault(p["company"], []).append(p["id"])
    target_after_tier2 = tier1_budget + tier2_budget
    for ids in by_company.values():
        n_cross = max(3, len(ids) // 3)
        attempts = 0
        added = 0
        while added < n_cross and attempts < n_cross * 20:
            attempts += 1
            if len(edges) >= target_after_tier2:
                break
            a, b = rng.sample(ids, 2)
            if by_id[a]["team"] == by_id[b]["team"]:
                continue
            if add(a, b, "intra_company"):
                added += 1
        if len(edges) >= target_after_tier2:
            break

    # Tier 3 — cross-company: ex-colleagues, conference ties. Sparse.
    # Fill up to exactly N_EDGES.
    all_ids = [p["id"] for p in people]
    safety = 0
    while len(edges) < N_EDGES and safety < 100_000:
        safety += 1
        a, b = rng.sample(all_ids, 2)
        if by_id[a]["company"] == by_id[b]["company"]:
            continue
        add(a, b, "cross_company")

    while len(edges) > N_EDGES:
        edges.pop(next(iter(edges)))
    while len(edges) < N_EDGES and safety < 200_000:
        safety += 1
        a, b = rng.sample(all_ids, 2)
        add(a, b, "cross_company")

    return sorted((a, b, s) for (a, b), s in edges.items())


INTERACTION_COLUMNS = (
    "twitter_likes", "twitter_comments", "twitter_reposts",
    "farcaster_recasts", "farcaster_replies",
    "linkedin_recommendations", "linkedin_endorsements",
    "debank_transactions",
)

# Per-type scoring band (spec): 1-5→+1, 6-20→+3, 21-50→+5, 51+→+7.
# Sum across columns, cap at 15.
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


def _interaction_score(counts: dict[str, int]) -> int:
    total = sum(_score_for_count(counts.get(c, 0)) for c in INTERACTION_COLUMNS)
    return min(total, INTERACTION_SCORE_CAP)


def _generate_interaction_counts(handles: dict, rng: random.Random) -> dict[str, int]:
    """Realistic fake counts — zero if the person has no handle on the
    relevant platform."""
    c = {col: 0 for col in INTERACTION_COLUMNS}
    if handles.get("twitter"):
        c["twitter_likes"]    = rng.choices([0, 3, 12, 35, 75], [0.1, 0.3, 0.3, 0.2, 0.1])[0]
        c["twitter_comments"] = rng.choices([0, 2, 8, 25, 60],  [0.15, 0.35, 0.3, 0.15, 0.05])[0]
        c["twitter_reposts"]  = rng.choices([0, 2, 7, 22],      [0.2, 0.4, 0.3, 0.1])[0]
    if handles.get("farcaster"):
        c["farcaster_recasts"] = rng.choices([0, 2, 8, 25], [0.2, 0.4, 0.3, 0.1])[0]
        c["farcaster_replies"] = rng.choices([0, 3, 10, 30, 55], [0.15, 0.35, 0.3, 0.15, 0.05])[0]
    if handles.get("linkedin"):
        c["linkedin_recommendations"] = rng.choices([0, 1, 3, 6], [0.55, 0.3, 0.12, 0.03])[0]
        c["linkedin_endorsements"]    = rng.choices([0, 2, 8, 22, 60], [0.2, 0.35, 0.25, 0.15, 0.05])[0]
    if handles.get("wallet"):  # mapped to debank column on write
        c["debank_transactions"] = rng.choices([0, 1, 5, 15, 40, 80], [0.3, 0.25, 0.2, 0.15, 0.08, 0.02])[0]
    return c


def _random_last_interaction(rng: random.Random) -> str:
    from datetime import datetime, timedelta
    return (datetime.now() - timedelta(days=rng.randint(0, 90))).date().isoformat()


def generate_identities(rng: random.Random, people: list[dict]) -> list[dict]:
    """Emit one wide-format identities row per person.

    Row shape: `{person_id, twitter, farcaster, linkedin, debank,
    twitter_likes, …, interaction_score, last_interaction}`. Empty
    strings for platforms the person doesn't own. Counts are zero on
    platforms the person isn't on.
    """
    rows: list[dict] = []
    for p in people:
        handles = p["handles"]
        counts = _generate_interaction_counts(handles, rng)
        score = _interaction_score(counts)
        row = {
            "person_id": p["id"],
            "twitter": handles.get("twitter", ""),
            "farcaster": handles.get("farcaster", ""),
            "linkedin": handles.get("linkedin", ""),
            # Internal platform name is `wallet`; CSV column is `debank`.
            "debank": handles.get("wallet", ""),
            **counts,
            "last_interaction": _random_last_interaction(rng) if any(counts.values()) else "",
            "interaction_score": score,
        }
        rows.append(row)
    return rows


WIDE_IDENTITY_COLUMNS = (
    "person_id", "twitter", "farcaster", "linkedin", "debank",
    *INTERACTION_COLUMNS,
    "last_interaction", "interaction_score",
)


def write_identities(path: str, rows: list[dict]) -> None:
    """Write identities.csv in wide format (one row per person)."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(WIDE_IDENTITY_COLUMNS))
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_people(path: str, people: list[dict]) -> None:
    """Write only the `id` column. No names, companies, teams, or roles."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id"])
        for p in people:
            w.writerow([p["id"]])


def write_edges(path: str, edges: list[tuple[str, str, int]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["from", "to", "strength"])
        for a, b, s in edges:
            w.writerow([a, b, s])


def summarize(people: list[dict], edges: list[tuple[str, str, int]]) -> None:
    by_id = {p["id"]: p for p in people}
    tiers = {"intra_team": [], "intra_company": [], "cross_company": []}
    for a, b, s in edges:
        pa, pb = by_id[a], by_id[b]
        if pa["team"] == pb["team"]:
            tiers["intra_team"].append(s)
        elif pa["company"] == pb["company"]:
            tiers["intra_company"].append(s)
        else:
            tiers["cross_company"].append(s)
    degrees: dict[str, int] = {p["id"]: 0 for p in people}
    for a, b, _ in edges:
        degrees[a] += 1
        degrees[b] += 1
    deg_vals = sorted(degrees.values())
    isolated = sum(1 for d in deg_vals if d == 0)
    # Per-platform totals: primary (priority-chosen => id prefix) and
    # raw handle counts across the wide-format identity rows. Primary
    # sums to len(people); raw sums can exceed that because each person
    # can own multiple platforms.
    primary_counts: dict[str, int] = {}
    raw_counts: dict[str, int] = {}
    for p in people:
        primary_counts[p["primary"]] = primary_counts.get(p["primary"], 0) + 1
        for plat in p["handles"]:
            raw_counts[plat] = raw_counts.get(plat, 0) + 1
    print(f"people       : {len(people)} (all with >=1 platform)")
    order = ["twitter", "farcaster", "linkedin", "wallet"]
    print(f"  primary platform (id prefix):")
    for k in order:
        if k in primary_counts:
            print(f"    {k:<12}: {primary_counts[k]}")
    print(f"  handles owned (across all platforms):")
    for k in order:
        if k in raw_counts:
            print(f"    {k:<12}: {raw_counts[k]}")
    print(f"edges        : {len(edges)}")
    for tier, values in tiers.items():
        if not values:
            print(f"  {tier:<14}: 0")
            continue
        avg = sum(values) / len(values)
        print(
            f"  {tier:<14}: {len(values):>3} edges, "
            f"strength min/avg/max = {min(values)}/{avg:.1f}/{max(values)}"
        )
    print(
        f"degree min/median/max: {deg_vals[0]}/"
        f"{deg_vals[len(deg_vals) // 2]}/{deg_vals[-1]}"
    )
    print(f"isolated nodes: {isolated}")


def _boost_edges(
    edges: list[tuple[str, str, int]],
    identities: list[dict],
) -> list[tuple[str, str, int]]:
    """Apply interaction_score boost to edge strengths, capped at 15.

    For edge (A,B), final = min(base + max(A.score, B.score), 15).
    Using MAX (not sum/avg) keeps the cap meaningful when both
    endpoints are highly active.
    """
    scores = {r["person_id"]: int(r.get("interaction_score", 0) or 0) for r in identities}
    boosted: list[tuple[str, str, int]] = []
    for a, b, s in edges:
        boost = max(scores.get(a, 0), scores.get(b, 0))
        boosted.append((a, b, min(int(s) + boost, 15)))
    return boosted


def main() -> None:
    rng = random.Random(SEED)
    people, team_members = generate_people(rng)
    edges = generate_edges(rng, people, team_members)
    identities = generate_identities(rng, people)
    edges = _boost_edges(edges, identities)
    write_people("people.csv", people)
    write_edges("edges.csv", edges)
    write_identities("identities.csv", identities)
    summarize(people, edges)
    total_handles = sum(
        sum(1 for v in r.values() if v and v != r["person_id"])
        for r in identities
    )
    print(
        f"identities   : {len(identities)} rows (wide format, 1 per person); "
        f"{total_handles} platform handles total"
    )


if __name__ == "__main__":
    main()
