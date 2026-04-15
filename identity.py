"""Identity resolution layer.

Future-proofing module. Today a person is a row in `people.csv`; tomorrow
a person will be the merged identity of multiple platform handles
(Twitter + LinkedIn + Farcaster + a wallet, etc).

This module defines the *interfaces* future ingestion layers will plug
into. Two implementations ship today:

- `ManualCSVResolver`  — reads explicit `person_id,platform,handle` rows
                         from an identities CSV. Useful for hand-curated
                         mappings and as a stable baseline for testing.
- `HeuristicResolver`  — placeholder. Will eventually merge accounts by
                         shared bio links, ENS records, on-chain proofs,
                         etc. Currently returns no merges.

Plus a stub `connection_extractor` for future API/scraper plug-ins.

Safety
------
None of these resolvers fabricate identities. The manual resolver only
maps what the user explicitly tells it. The heuristic resolver is a
no-op until real signals are wired in.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from typing import Iterable, Protocol

from core import Person, Relationship, SocialAccount


@dataclass
class IdentityCluster:
    """A group of accounts that all belong to the same person."""

    person_id: str
    account_ids: list[str] = field(default_factory=list)


class IdentityResolver(Protocol):
    """Anything that can group accounts into per-person clusters."""

    def resolve(
        self,
        people: list[Person],
        accounts: list[SocialAccount],
    ) -> list[IdentityCluster]: ...


class ManualCSVResolver:
    """Resolver backed by explicit `person_id,platform,handle` rows.

    The CSVRepository already attaches accounts to people when an
    `identities.csv` is supplied; this resolver just buckets them.
    Use it as the canonical "ground-truth" resolver for tests and
    hand-curated mappings.
    """

    def __init__(self, identities_path: str | None = None) -> None:
        self.identities_path = identities_path

    def resolve(
        self,
        people: list[Person],
        accounts: list[SocialAccount],
    ) -> list[IdentityCluster]:
        clusters: dict[str, IdentityCluster] = {}
        for acc in accounts:
            if acc.owner_person_id is None:
                continue
            cluster = clusters.setdefault(
                acc.owner_person_id,
                IdentityCluster(person_id=acc.owner_person_id),
            )
            cluster.account_ids.append(acc.id)
        return list(clusters.values())


class HeuristicResolver:
    """Placeholder for automated identity inference.

    Future signals: matching ENS / SIWE proofs, identical bio links,
    cross-platform display-name + photo similarity, on-chain wallet
    overlap. Until those are wired in this returns no clusters — the
    system gracefully treats every account as un-merged.
    """

    def resolve(
        self,
        people: list[Person],
        accounts: list[SocialAccount],
    ) -> list[IdentityCluster]:
        return []


# --- Connection extractor stub ----------------------------------------


def connection_extractor(
    source: str,
    handle: str,
) -> Iterable[Relationship]:
    """Placeholder for platform-specific connection ingestion.

    Future implementations will fetch followers/following from Twitter,
    1st-degree connections from LinkedIn, Farcaster channel co-membership,
    or wallet-to-wallet transactions. Each implementation must yield
    `Relationship` objects with `kind` set appropriately and `source` set
    to the platform name.

    Today: raises `NotImplementedError` with a hint about which adapter
    will own the platform. Catch and skip in callers — never fabricate.
    """
    raise NotImplementedError(
        f"connection_extractor for source={source!r} not implemented yet. "
        f"Plug in a platform-specific adapter (twitter, linkedin, "
        f"farcaster, wallet) to ingest real edges."
    )
