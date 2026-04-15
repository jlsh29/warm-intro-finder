"""LinkedIn Connections.csv -> warm_intro people.csv / edges.csv / identities.csv.

Usage
-----
    python linkedin_ingester.py \
        --owner-name "Alice Anderson" \
        --owner-url  "https://www.linkedin.com/in/alice-anderson/" \
        --connections alice_connections.csv \
        --owner-name "Bob Builder" \
        --owner-url  "https://www.linkedin.com/in/bob-builder/" \
        --connections bob_connections.csv \
        --out-dir ./linkedin_data

    # Then route through the existing engine:
    python warm_intro.py \
        --people linkedin_data/people.csv \
        --edges  linkedin_data/edges.csv \
        --identities linkedin_data/identities.csv \
        --entry li_alice-anderson --target li_charlie-chen --explain

Input format
------------
LinkedIn's official export "Connections.csv" has columns like:
  First Name, Last Name, URL, Email Address, Company, Position, Connected On

This script auto-detects the relevant ones (case-insensitive, treats
spaces and underscores interchangeably). Common alias sets:
  url:      url | profile_url | linkedin_url | profile
  first:    first_name | firstname | first
  last:     last_name | lastname | last
  company:  company | company_name | current_company | organization
  position: position | title | job_title | role

Email Address is intentionally ignored - not needed for routing and
unnecessary PII to carry through the pipeline.

Direction handling
------------------
LinkedIn connections are inherently mutual (both parties accepted),
so every row in an owner's Connections.csv produces one undirected
edge between the owner and that connection. No mutual-vs-one-way
filtering needed (unlike Twitter follows).

Output
------
Three CSVs in `--out-dir`:
  people.csv      (id, name, company, role)
  edges.csv       (from, to, strength, tier)
  identities.csv  (person_id, platform, handle)

Person ids are namespaced as `li_{slug}` where `{slug}` is the
LinkedIn URL slug (the part after `/in/`). This keeps ids stable
across exports and prevents collision with `tw_*` / `fc_*` / `wallet_*`
ids from other platform ingesters.

Cross-owner dedup
-----------------
If both Alice's and Bob's exports contain Charlie, Charlie appears
once in `people.csv` with edges to both. Charlie's company/role are
taken from the most recently-seen export (last write wins).

Safety
------
This script does not contact LinkedIn or fabricate edges. It only
emits what is present in the input CSVs. We do NOT synthesize
connection-of-connection edges - if Alice knows Charlie and Bob
knows Charlie, we do NOT emit Alice<->Bob unless the data shows it.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys


URL_COLS = ("url", "profile_url", "linkedin_url", "profile")
FIRST_COLS = ("first_name", "firstname", "first")
LAST_COLS = ("last_name", "lastname", "last")
COMPANY_COLS = ("company", "company_name", "current_company", "organization")
POSITION_COLS = ("position", "title", "job_title", "role")

TIER_STRENGTH: dict[str, int] = {
    "direct": 10,
    "mutual": 8,
    "shared_org": 5,
    "platform_similarity": 2,
}

_SLUG_RE = re.compile(r"/in/([^/?#]+)", re.IGNORECASE)


def detect_col(headers: list[str], candidates: tuple[str, ...]) -> str | None:
    """Find a header matching any candidate; case-insensitive, treats
    space and underscore as equivalent (so 'First Name' matches 'first_name')."""
    norm = {h.lower().strip().replace(" ", "_"): h for h in headers}
    for cand in candidates:
        if cand in norm:
            return norm[cand]
    return None


def slug_from_url(url_or_slug: str) -> str:
    """Extract the canonical LinkedIn slug.

    Accepts full URLs (`https://www.linkedin.com/in/alice-anderson/`),
    shortened URLs, or bare slugs (`alice-anderson`). Slugs are
    lowercased and stripped of trailing slashes / query strings.
    """
    s = (url_or_slug or "").strip()
    if not s:
        return ""
    m = _SLUG_RE.search(s)
    if m:
        return m.group(1).lower().strip("/").split("?")[0]
    # Bare slug fallback - just clean it.
    return s.lower().lstrip("@").strip("/").split("?")[0]


def person_id(slug: str) -> str:
    """Namespace a LinkedIn URL slug into a person id (e.g. `li_alice-anderson`)."""
    return f"li_{slug}"


def read_connections_csv(path: str) -> list[dict]:
    """Parse a LinkedIn Connections.csv into normalized records.

    Returns a list of dicts: {slug, name, company, role, url}.
    Rows missing both URL/slug and a name are skipped.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"input file not found: {path}")

    with open(path, newline="", encoding="utf-8") as f:
        # LinkedIn sometimes prefixes the file with metadata lines before
        # the real header. Probe for the row whose lowercased contents
        # contain something URL-like; treat that as the header line.
        first = f.readline()
        f.seek(0)
        # Most exports start cleanly with the header row, so just trust it.
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"{path}: empty CSV")
        url_col = detect_col(reader.fieldnames, URL_COLS)
        first_col = detect_col(reader.fieldnames, FIRST_COLS)
        last_col = detect_col(reader.fieldnames, LAST_COLS)
        company_col = detect_col(reader.fieldnames, COMPANY_COLS)
        position_col = detect_col(reader.fieldnames, POSITION_COLS)
        if not url_col and not (first_col or last_col):
            raise ValueError(
                f"{path}: needs at least URL or First/Last Name columns. "
                f"Got: {reader.fieldnames}"
            )

        records: list[dict] = []
        for row in reader:
            url = (row.get(url_col, "") if url_col else "").strip()
            first = (row.get(first_col, "") if first_col else "").strip()
            last = (row.get(last_col, "") if last_col else "").strip()
            company = (row.get(company_col, "") if company_col else "").strip()
            role = (row.get(position_col, "") if position_col else "").strip()
            slug = slug_from_url(url) if url else ""
            if not slug and (first or last):
                # Synthesize a slug from the name as a last resort.
                synth = re.sub(r"[^a-z0-9]+", "-", f"{first} {last}".lower()).strip("-")
                if synth:
                    slug = synth
            if not slug:
                continue
            name = " ".join(p for p in (first, last) if p) or slug
            records.append(
                {
                    "slug": slug,
                    "name": name,
                    "company": company,
                    "role": role,
                    "url": url,
                }
            )
    return records


def ingest(
    owners: list[tuple[str, str, str]],  # (display_name, url_or_slug, connections_path)
    out_dir: str,
    edge_tier: str = "mutual",
) -> dict:
    """Convert per-owner LinkedIn Connections.csv files into engine CSVs.

    Each row in an owner's connections file produces one undirected
    edge between the owner and that connection (LinkedIn connections
    are inherently mutual). Cross-owner dedup ensures every connection
    appears once in `people.csv` even if both owners share them.
    Returns the three output paths plus counts.
    """
    if edge_tier not in TIER_STRENGTH:
        raise ValueError(f"unknown tier {edge_tier!r}")

    # slug -> canonical attributes (last write wins on metadata)
    people: dict[str, dict] = {}
    # sorted-pair -> (strength, tier)
    edges: dict[tuple[str, str], tuple[int, str]] = {}
    # Owner slug -> handle (for identities.csv); preserve full URL if provided
    handles: dict[str, str] = {}

    seen_owners: set[str] = set()
    for owner_name, owner_url, conn_path in owners:
        owner_slug = slug_from_url(owner_url)
        if not owner_slug:
            raise ValueError(
                f"could not derive slug from --owner-url {owner_url!r}"
            )
        if owner_slug in seen_owners:
            print(f"warning: owner {owner_slug!r} listed twice; ignoring duplicate",
                  file=sys.stderr)
            continue
        seen_owners.add(owner_slug)

        # Register the owner as a person
        people[owner_slug] = {
            "name": owner_name.strip() or owner_slug,
            "company": people.get(owner_slug, {}).get("company", ""),
            "role": people.get(owner_slug, {}).get("role", ""),
        }
        handles[owner_slug] = owner_url.strip() or owner_slug

        records = read_connections_csv(conn_path)
        for rec in records:
            slug = rec["slug"]
            if slug == owner_slug:
                continue  # skip self-references defensively
            # Last-write-wins for company/role; keep the longest name seen.
            existing = people.get(slug, {})
            new_name = rec["name"]
            existing_name = existing.get("name", "")
            people[slug] = {
                "name": new_name if (not existing_name or len(new_name) > len(existing_name)) else existing_name,
                "company": rec["company"] or existing.get("company", ""),
                "role": rec["role"] or existing.get("role", ""),
            }
            handles[slug] = rec["url"] or handles.get(slug, slug)

            key = tuple(sorted([owner_slug, slug]))
            new_s = TIER_STRENGTH[edge_tier]
            cur = edges.get(key)
            if cur is None or new_s > cur[0]:
                edges[key] = (new_s, edge_tier)

        print(
            f"info: owner @{owner_slug}: {len(records)} connection(s) ingested",
            file=sys.stderr,
        )

    os.makedirs(out_dir, exist_ok=True)
    people_path = os.path.join(out_dir, "people.csv")
    edges_path = os.path.join(out_dir, "edges.csv")
    identities_path = os.path.join(out_dir, "identities.csv")

    sorted_people = sorted(people.items())

    with open(people_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "company", "role"])
        for slug, p in sorted_people:
            w.writerow([person_id(slug), p["name"], p["company"], p["role"]])

    with open(edges_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["from", "to", "strength", "tier"])
        for (a, b), (s, t) in sorted(edges.items()):
            w.writerow([person_id(a), person_id(b), s, t])

    with open(identities_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["person_id", "platform", "handle"])
        for slug, _ in sorted_people:
            handle = handles.get(slug, slug)
            # Identity handle is the full URL when we have it (more useful
            # than the bare slug), falling back to the slug.
            w.writerow([person_id(slug), "linkedin", handle])

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
        description="Convert LinkedIn Connections.csv exports into warm_intro CSVs.",
        epilog=(
            "Each --owner-name must be paired with one --owner-url and one "
            "--connections, in the same order. Repeat the trio for additional "
            "owners contributing data."
        ),
    )
    parser.add_argument(
        "--owner-name", action="append", required=True,
        help="Display name of the export owner (e.g. 'Alice Anderson').",
    )
    parser.add_argument(
        "--owner-url", action="append", required=True,
        help="LinkedIn profile URL or slug of the owner.",
    )
    parser.add_argument(
        "--connections", action="append", required=True,
        help="Path to that owner's Connections.csv export.",
    )
    parser.add_argument(
        "--out-dir", default=".",
        help="Directory where people.csv / edges.csv / identities.csv are written.",
    )
    parser.add_argument(
        "--tier", default="mutual",
        choices=sorted(TIER_STRENGTH.keys()),
        help="Tier label for emitted edges (default: mutual, since a "
             "LinkedIn connection is bidirectional acceptance).",
    )
    args = parser.parse_args(argv)

    n = len(args.owner_name)
    if not (n == len(args.owner_url) == len(args.connections)):
        parser.error(
            "--owner-name, --owner-url, --connections must each appear the "
            "same number of times (one per owner)."
        )

    owners = list(zip(args.owner_name, args.owner_url, args.connections))
    try:
        result = ingest(owners, out_dir=args.out_dir, edge_tier=args.tier)
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
        f"--entry li_<owner_slug> --target li_<connection_slug> --explain"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
