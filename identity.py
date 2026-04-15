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
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable, Protocol

from core import Person, Relationship, RepositoryPayload, SocialAccount


@dataclass
class IdentityCluster:
    """A group of accounts that all belong to the same person."""

    person_id: str
    account_ids: list[str] = field(default_factory=list)


@dataclass
class MergeProposal:
    """A proposal to collapse multiple person ids into one canonical id.

    Phase B: emitted when two persons share an account on the same
    platform — strong evidence they're the same human. Future heuristics
    (ENS/SIWE proofs, bio-link matching, on-chain wallet co-spending)
    will emit additional proposals through this same shape.
    """

    canonical_id: str
    merged_ids: list[str]
    reason: str
    confidence: float = 1.0  # 1.0 = certain (shared account); <1 for heuristic


@dataclass
class ResolutionResult:
    """What an IdentityResolver returns: clusters + proposed merges."""

    clusters: list[IdentityCluster] = field(default_factory=list)
    merges: list[MergeProposal] = field(default_factory=list)


class IdentityResolver(Protocol):
    """Anything that can group accounts into per-person clusters and
    optionally propose person-level merges from an ingestion payload."""

    def resolve(self, payload: RepositoryPayload) -> ResolutionResult: ...


def _bucket_accounts(accounts: Iterable[SocialAccount]) -> list[IdentityCluster]:
    clusters: dict[str, IdentityCluster] = {}
    for acc in accounts:
        if acc.owner_person_id is None:
            continue
        clusters.setdefault(
            acc.owner_person_id, IdentityCluster(person_id=acc.owner_person_id)
        ).account_ids.append(acc.id)
    return list(clusters.values())


class ManualCSVResolver:
    """Manual mappings only — no merging.

    Buckets accounts by their already-assigned `owner_person_id`.
    Useful as a stable baseline that NEVER infers merges, e.g. for
    tests where you want the data exactly as authored.
    """

    def __init__(self, identities_path: str | None = None) -> None:
        self.identities_path = identities_path

    def resolve(self, payload: RepositoryPayload) -> ResolutionResult:
        return ResolutionResult(clusters=_bucket_accounts(payload.accounts))


class SharedAccountResolver:
    """Detects person-level merges via shared platform accounts.

    If two distinct person ids both claim ownership of the same account
    (e.g. both p001 and p042 list `twitter:@alice`), they're treated as
    the same human and merged. The lexicographically smallest id wins
    as canonical.

    This is deterministic ground-truth merging — confidence 1.0 — not
    a heuristic. Heuristic signals (bio-link matching, ENS proofs,
    similarity) belong in `HeuristicResolver`.
    """

    def resolve(self, payload: RepositoryPayload) -> ResolutionResult:
        # Union-find over person ids, joined whenever they share an account.
        owners_by_account: dict[str, set[str]] = defaultdict(set)
        for acc_id, pid in payload.account_claims:
            owners_by_account[acc_id].add(pid)

        parent: dict[str, str] = {}

        def find(x: str) -> str:
            while parent.get(x, x) != x:
                parent[x] = parent.get(parent[x], parent[x])
                x = parent[x]
            return x

        def union(a: str, b: str) -> None:
            ra, rb = find(a), find(b)
            if ra == rb:
                return
            # Smallest id wins as canonical
            if ra < rb:
                parent[rb] = ra
            else:
                parent[ra] = rb

        merge_evidence: dict[frozenset[str], list[str]] = defaultdict(list)
        for acc_id, owners in owners_by_account.items():
            if len(owners) < 2:
                continue
            owners_list = sorted(owners)
            for o in owners_list:
                parent.setdefault(o, o)
            for o in owners_list[1:]:
                union(owners_list[0], o)
            merge_evidence[frozenset(owners_list)].append(acc_id)

        # Materialize merge proposals from union-find components
        components: dict[str, set[str]] = defaultdict(set)
        for pid in parent:
            components[find(pid)].add(pid)

        merges: list[MergeProposal] = []
        for canonical, members in components.items():
            others = sorted(members - {canonical})
            if not others:
                continue
            # Build a reason string from the evidence
            shared_accs: set[str] = set()
            for owners_set, acc_ids in merge_evidence.items():
                if owners_set & members:
                    shared_accs.update(acc_ids)
            reason = (
                f"shared account(s): {', '.join(sorted(shared_accs)[:3])}"
                + ("..." if len(shared_accs) > 3 else "")
            )
            merges.append(
                MergeProposal(
                    canonical_id=canonical,
                    merged_ids=others,
                    reason=reason,
                    confidence=1.0,
                )
            )

        return ResolutionResult(
            clusters=_bucket_accounts(payload.accounts),
            merges=merges,
        )


class HeuristicResolver:
    """Placeholder for soft-signal identity inference.

    Future signals: matching ENS / SIWE proofs, identical bio links,
    cross-platform display-name + photo similarity, on-chain wallet
    co-transactions. Until those are wired in this returns the
    deterministic SharedAccountResolver result with no additional
    heuristics — keeping the behavior safe (no fabricated merges).
    """

    def __init__(self) -> None:
        self._inner = SharedAccountResolver()

    def resolve(self, payload: RepositoryPayload) -> ResolutionResult:
        return self._inner.resolve(payload)


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
