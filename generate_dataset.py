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


def unique_name(rng: random.Random, used: set[str]) -> str:
    for _ in range(100):
        name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
        if name not in used:
            used.add(name)
            return name
    suffix = 2
    base = name  # noqa: F821 — last attempted name
    while f"{base} {suffix}" in used:
        suffix += 1
    name = f"{base} {suffix}"
    used.add(name)
    return name


def generate_people(rng: random.Random) -> tuple[list[dict], dict[str, list[str]]]:
    people: list[dict] = []
    team_members: dict[str, list[str]] = {}
    used: set[str] = set()
    pid = 1

    for company, size in COMPANIES:
        n_teams = max(2, min(4, size // 6))
        team_sizes = [size // n_teams] * n_teams
        for i in range(size - sum(team_sizes)):
            team_sizes[i] += 1
        for t_idx, t_size in enumerate(team_sizes, start=1):
            team_key = f"{company} / Team {t_idx}"
            team_members[team_key] = []
            for _ in range(t_size):
                person_id = f"p{pid:03d}"
                pid += 1
                person = {
                    "id": person_id,
                    "name": unique_name(rng, used),
                    "company": company,
                    "team": team_key,
                    "role": rng.choice(ROLES),
                }
                people.append(person)
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


def _slugify_part(part: str) -> str:
    """Normalize a name fragment into a handle-safe token: lowercase a-z0-9 only."""
    return re.sub(r"[^a-z0-9]+", "", part.lower())


def _split_name(full: str) -> tuple[str, str]:
    """Split 'First Last' (or 'First Last 2' dedup variants) into (first, rest).

    `rest` is normalized to a single handle-safe token so 'Anderson 2'
    becomes 'anderson2'. For names with multiple last-name words we
    join them without a separator to keep handles short.
    """
    parts = full.strip().split()
    if not parts:
        return "", ""
    first = _slugify_part(parts[0])
    rest = "".join(_slugify_part(p) for p in parts[1:]) if len(parts) > 1 else ""
    return first, rest


def generate_identities(
    rng: random.Random, people: list[dict]
) -> list[tuple[str, str, str, str]]:
    """Emit 3 identity rows per person (twitter, linkedin, farcaster).

    Returns list of (person_id, platform, handle, dm) where dm is
    "yes" or "no" randomly per row. Handles are deduped per platform
    using numeric suffixes so two people with the same first name (or
    the same first+last) still get unique handles on each platform.
    """
    # Per-platform seen-handle sets for dedup.
    seen = {"twitter": set(), "linkedin": set(), "farcaster": set()}

    def claim(platform: str, base: str) -> str:
        """Return the first unused handle at or after `base` on `platform`."""
        if base and base not in seen[platform]:
            seen[platform].add(base)
            return base
        # Append a numeric suffix until we find a free slot.
        n = 2
        while True:
            candidate = f"{base}{n}"
            if candidate not in seen[platform]:
                seen[platform].add(candidate)
                return candidate
            n += 1

    rows: list[tuple[str, str, str, str]] = []
    for p in people:
        first, rest = _split_name(p["name"])
        if not first:
            # Defensive: no name to derive a handle from. Fall back to id.
            first = _slugify_part(p["id"]) or p["id"]
        combined = f"{first}_{rest}" if rest else first
        linked = f"{first}-{rest}" if rest else first

        tw = claim("twitter", combined)
        li = claim("linkedin", linked)
        fc = claim("farcaster", first)

        # Spec: twitter handles carry the `@`, linkedin and farcaster
        # do not. The Flask UI's link_for() strips `@` where it's not
        # wanted, so either would work, but we match the spec literally.
        for platform, handle in (
            ("twitter", f"@{tw}"),
            ("linkedin", li),
            ("farcaster", fc),
        ):
            # DM availability varies per platform/person; 50% coin flip
            # per row so the data has realistic variance.
            dm = "yes" if rng.random() < 0.5 else "no"
            rows.append((p["id"], platform, handle, dm))
    return rows


def write_identities(path: str, rows: list[tuple[str, str, str, str]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["person_id", "platform", "handle", "dm"])
        for r in rows:
            w.writerow(list(r))


def write_people(path: str, people: list[dict]) -> None:
    # (identity helpers live above; keeping write_* together here)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "company", "team", "role"])
        for p in people:
            w.writerow([p["id"], p["name"], p["company"], p["team"], p["role"]])


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
    print(f"people       : {len(people)}")
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


def main() -> None:
    rng = random.Random(SEED)
    people, team_members = generate_people(rng)
    edges = generate_edges(rng, people, team_members)
    identities = generate_identities(rng, people)
    write_people("people.csv", people)
    write_edges("edges.csv", edges)
    write_identities("identities.csv", identities)
    summarize(people, edges)
    dm_yes = sum(1 for _, _, _, dm in identities if dm == "yes")
    print(
        f"identities   : {len(identities)} rows "
        f"(3 per person; {dm_yes} with DM=yes, {len(identities) - dm_yes} with DM=no)"
    )


if __name__ == "__main__":
    main()
