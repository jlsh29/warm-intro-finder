"""Core domain model and data-layer abstraction.

This module introduces a platform-agnostic data model for the warm-intro
engine. The runtime path-finding code (warm_intro.py) still operates over
a flat adjacency list keyed by person id — but graph data now flows
through a `GraphRepository` interface, so future ingestion sources
(LinkedIn / Twitter / Farcaster / on-chain wallets / DBs) can be added
without touching pathfinding.

Entities
--------
- Person          — a real human, the routing target
- SocialAccount   — a handle on a platform (Twitter, LinkedIn, …) owned
                    by a Person
- Relationship    — typed edge between two entities

Repositories
------------
- GraphRepository  (Protocol)  — anything that can produce the entity lists
- CSVRepository    (concrete)  — reads the existing CSV format

The repository returns *entities*, not an adjacency list. The engine
constructs its working graph from those entities. New backends just need
to populate Person/SocialAccount/Relationship — no algorithm changes.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from typing import Iterable, Protocol


# --- Entities ----------------------------------------------------------

PLATFORM_UNKNOWN = "unknown"


@dataclass
class SocialAccount:
    """A handle on a platform (Twitter, LinkedIn, Farcaster, wallet, …)."""

    id: str  # globally unique account id, e.g. "twitter:@alice"
    platform: str  # "twitter", "linkedin", "farcaster", "wallet", …
    handle: str  # the human-facing identifier on that platform
    owner_person_id: str | None = None  # set after identity resolution
    attributes: dict[str, str] = field(default_factory=dict)


@dataclass
class Person:
    """A real human. The primary routing entity."""

    id: str
    name: str
    accounts: list[SocialAccount] = field(default_factory=list)
    attributes: dict[str, str] = field(default_factory=dict)


# Relationship "kinds" describe what the edge represents. Strength is
# numeric (higher = stronger); the kind lets downstream code build
# explanations like "shared org via Helix Labs".
RELATIONSHIP_KINDS = (
    "person-person",      # explicit knows / mutual contact
    "person-account",     # ownership: person owns this account
    "account-account",    # cross-platform identity match
    "shared-organization",  # both at same company / community
    "platform-similarity",  # weak signal (followers / cluster)
)


@dataclass
class Relationship:
    """Typed edge between two entity ids."""

    from_id: str
    to_id: str
    kind: str  # one of RELATIONSHIP_KINDS
    strength: float = 1.0
    source: str = ""  # e.g. "csv", "twitter-api", "manual-mapping"
    attributes: dict[str, str] = field(default_factory=dict)


# --- Repository interface ---------------------------------------------


@dataclass
class RepositoryPayload:
    """What every GraphRepository.load() must return.

    `account_claims` carries the raw (account_id, person_id) pairs as
    seen at ingestion time — including duplicates where two persons
    claim the same account. The identity layer uses these to propose
    person-level merges (Phase B).
    """

    people: list[Person]
    accounts: list[SocialAccount]
    relationships: list[Relationship]
    account_claims: list[tuple[str, str]] = field(default_factory=list)


def apply_merges(
    payload: RepositoryPayload,
    merges: "list[MergeProposal]",  # forward ref, defined in identity.py
) -> RepositoryPayload:
    """Collapse `merged_ids` into `canonical_id` across the entire payload.

    - Person entities for merged ids are dropped; their accounts move to
      the canonical Person.
    - Relationships with merged endpoints are rewritten to the canonical
      id. Self-edges are dropped. Duplicate edges keep the max strength.
    - Account ownership is rewritten to the canonical id.

    Idempotent: applying the same merges twice is a no-op.
    """
    if not merges:
        return payload

    # Build id -> canonical map
    canonical: dict[str, str] = {}
    for m in merges:
        for mid in m.merged_ids:
            canonical[mid] = m.canonical_id
    if not canonical:
        return payload

    def canon(pid: str) -> str:
        seen: set[str] = set()
        while pid in canonical and pid not in seen:
            seen.add(pid)
            pid = canonical[pid]
        return pid

    # People: keep only canonical persons; aggregate accounts onto them
    by_id = {p.id: p for p in payload.people}
    surviving: dict[str, Person] = {}
    for p in payload.people:
        cid = canon(p.id)
        if cid not in by_id:
            # canonical id not present; treat as identity
            cid = p.id
        if cid not in surviving:
            target = by_id.get(cid, p)
            surviving[cid] = Person(
                id=target.id,
                name=target.name,
                accounts=[],
                attributes=dict(target.attributes),
            )
    for p in payload.people:
        cid = canon(p.id)
        if cid not in surviving:
            continue
        for acc in p.accounts:
            new_acc = SocialAccount(
                id=acc.id,
                platform=acc.platform,
                handle=acc.handle,
                owner_person_id=cid,
                attributes=dict(acc.attributes),
            )
            # Dedup accounts already attached
            if not any(a.id == new_acc.id for a in surviving[cid].accounts):
                surviving[cid].accounts.append(new_acc)

    new_people = list(surviving.values())

    # Accounts: rewrite owners and dedupe
    seen_acc: dict[str, SocialAccount] = {}
    for acc in payload.accounts:
        owner = canon(acc.owner_person_id) if acc.owner_person_id else None
        if acc.id in seen_acc:
            continue
        seen_acc[acc.id] = SocialAccount(
            id=acc.id,
            platform=acc.platform,
            handle=acc.handle,
            owner_person_id=owner,
            attributes=dict(acc.attributes),
        )
    new_accounts = list(seen_acc.values())

    # Relationships: rewrite endpoints, drop self-edges, dedupe
    survivor_ids = {p.id for p in new_people}
    seen_rel: dict[tuple[str, str, str], Relationship] = {}
    for rel in payload.relationships:
        a = canon(rel.from_id)
        b = canon(rel.to_id)
        if a == b:
            continue
        if rel.kind == "person-person" and (
            a not in survivor_ids or b not in survivor_ids
        ):
            continue
        key_pair = (a, b) if a < b else (b, a)
        key = (key_pair[0], key_pair[1], rel.kind)
        existing = seen_rel.get(key)
        if existing is None:
            seen_rel[key] = Relationship(
                from_id=key_pair[0],
                to_id=key_pair[1],
                kind=rel.kind,
                strength=rel.strength,
                source=rel.source,
                attributes=dict(rel.attributes),
            )
        else:
            if rel.strength > existing.strength:
                existing.strength = rel.strength
                existing.source = rel.source
    new_rels = list(seen_rel.values())

    new_claims = [
        (acc_id, canon(pid)) for acc_id, pid in payload.account_claims
    ]

    return RepositoryPayload(
        people=new_people,
        accounts=new_accounts,
        relationships=new_rels,
        account_claims=new_claims,
    )


class GraphRepository(Protocol):
    """Anything that can produce the entity tuple."""

    def load(self) -> RepositoryPayload: ...


# --- CSV implementation -----------------------------------------------


META_FIELDS = ("company", "team", "role")


class CSVRepository:
    """Reads the project's existing CSV format.

    Backwards compatible: works on `people.csv` files with no platform
    columns and `edges.csv` files with no `strength` column. It treats
    the data as a single-platform graph of Person nodes only — no
    SocialAccount entities are produced unless an `identities.csv` is
    also supplied.

    Parameters
    ----------
    people_path
        Path to `people.csv` (`id,name,[company,team,role,...]`).
    edges_path
        Path to `edges.csv` (`from,to,[strength]`).
    identities_path
        Optional path to a CSV with rows `person_id,platform,handle`
        used to populate `SocialAccount` entities. Each row produces
        one account owned by the named person.
    """

    DEFAULT_STRENGTH = 1.0

    def __init__(
        self,
        people_path: str,
        edges_path: str,
        identities_path: str | None = None,
    ) -> None:
        self.people_path = people_path
        self.edges_path = edges_path
        self.identities_path = identities_path

    def load(self) -> RepositoryPayload:
        people = self._load_people()
        accounts, claims = self._load_accounts(people)
        relationships = self._load_relationships(people)
        return RepositoryPayload(
            people=people,
            accounts=accounts,
            relationships=relationships,
            account_claims=claims,
        )

    def _load_people(self) -> list[Person]:
        people: list[Person] = []
        seen: set[str] = set()
        with open(self.people_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or "id" not in reader.fieldnames:
                raise ValueError(
                    f"{self.people_path}: expected header with at least 'id' column"
                )
            has_name = "name" in reader.fieldnames
            for row in reader:
                pid = (row["id"] or "").strip()
                if not pid:
                    continue
                if pid in seen:
                    raise ValueError(
                        f"{self.people_path}: duplicate id {pid!r}"
                    )
                seen.add(pid)
                name = (row["name"].strip() if has_name and row["name"] else pid)
                attrs = {
                    k: (row[k].strip() if row.get(k) else "") for k in META_FIELDS
                }
                people.append(Person(id=pid, name=name, attributes=attrs))
        return people

    def _load_accounts(
        self, people: list[Person]
    ) -> tuple[list[SocialAccount], list[tuple[str, str]]]:
        if not self.identities_path:
            return [], []
        by_id = {p.id: p for p in people}
        accounts: dict[str, SocialAccount] = {}
        claims: list[tuple[str, str]] = []
        with open(self.identities_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            required = {"person_id", "platform", "handle"}
            if not reader.fieldnames or required - set(reader.fieldnames):
                raise ValueError(
                    f"{self.identities_path}: expected columns "
                    f"{sorted(required)}"
                )
            for row in reader:
                pid = (row["person_id"] or "").strip()
                platform = (row["platform"] or "").strip().lower()
                handle = (row["handle"] or "").strip()
                if not pid or not platform or not handle:
                    continue
                if pid not in by_id:
                    continue
                acc_id = f"{platform}:{handle}"
                claims.append((acc_id, pid))
                if acc_id in accounts:
                    # Subsequent claims for the same account are tracked
                    # in `claims` so the identity layer can detect cross-
                    # person overlap and propose a merge. We do NOT
                    # silently overwrite the first claim's owner here.
                    continue
                account = SocialAccount(
                    id=acc_id,
                    platform=platform,
                    handle=handle,
                    owner_person_id=pid,
                )
                accounts[acc_id] = account
                by_id[pid].accounts.append(account)
        return list(accounts.values()), claims

    def _load_relationships(self, people: list[Person]) -> list[Relationship]:
        known = {p.id for p in people}
        rels: list[Relationship] = []
        with open(self.edges_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or {"from", "to"} - set(reader.fieldnames):
                raise ValueError(
                    f"{self.edges_path}: expected header with 'from' and 'to' columns"
                )
            has_strength = "strength" in reader.fieldnames
            seen: set[tuple[str, str]] = set()
            for row in reader:
                a = (row["from"] or "").strip()
                b = (row["to"] or "").strip()
                if not a or not b or a == b:
                    continue
                if a not in known or b not in known:
                    continue
                key = (a, b) if a < b else (b, a)
                if key in seen:
                    continue
                seen.add(key)
                strength = self.DEFAULT_STRENGTH
                if has_strength:
                    raw = (row.get("strength") or "").strip()
                    try:
                        strength = float(raw) if raw else self.DEFAULT_STRENGTH
                    except ValueError:
                        strength = self.DEFAULT_STRENGTH
                    if strength <= 0:
                        strength = self.DEFAULT_STRENGTH
                rels.append(
                    Relationship(
                        from_id=a,
                        to_id=b,
                        kind="person-person",
                        strength=strength,
                        source="csv",
                    )
                )
        return rels


# --- Convenience -------------------------------------------------------


def people_to_lookups(
    people: Iterable[Person],
) -> tuple[dict[str, str], dict[str, list[str]], dict[str, dict[str, str]]]:
    """Flatten a Person iterable into the (id_to_name, name_to_ids,
    id_to_meta) tuple used by the runtime Graph."""
    id_to_name: dict[str, str] = {}
    name_to_ids: dict[str, list[str]] = {}
    id_to_meta: dict[str, dict[str, str]] = {}
    for p in people:
        id_to_name[p.id] = p.name
        name_to_ids.setdefault(p.name.lower(), []).append(p.id)
        id_to_meta[p.id] = dict(p.attributes)
    return id_to_name, name_to_ids, id_to_meta
