"""Warm Introduction Path Finder.

CLI tool that finds the shortest warm-introduction path from one or more entry
points to a target person in a social graph.

CSV input format
----------------
people.csv  -- columns: id,name        (id is the canonical key)
edges.csv   -- columns: from,to        (one row per connection; undirected)

Usage
-----
    python warm_intro.py \
        --people people.csv \
        --edges  edges.csv \
        --entry  alice,bob \
        --target zoe \
        [--top-k 3]

Entry/target values may be either an `id` from people.csv or a `name`
(case-insensitive). Names are resolved to ids; ambiguous names raise an error.
"""

from __future__ import annotations

import argparse
import json
import sys
import heapq
import math
from collections import defaultdict
from dataclasses import dataclass, field

from core import (
    CSVRepository,
    GraphRepository,
    META_FIELDS,
    Person,
    Relationship,
    RepositoryPayload,
    SocialAccount,
    TIER_STRENGTH,
    apply_merges,
    derive_shared_org_relationships,
    people_to_lookups,
    reason_for,
)
from identity import (
    IdentityCluster,
    IdentityResolver,
    ManualCSVResolver,
    MergeProposal,
    SharedAccountResolver,
)


DEFAULT_STRENGTH = 1.0
EDGE_DIST_TOL = 1e-9


@dataclass
class Graph:
    adjacency: dict[str, set[str]]
    id_to_name: dict[str, str]
    name_to_ids: dict[str, list[str]]
    id_to_meta: dict[str, dict[str, str]]
    edge_strength: dict[tuple[str, str], float] = field(default_factory=dict)
    # Phase A additions — populated for repositories that emit them; the
    # pathfinder ignores these today but downstream consumers (UI, JSON
    # output) can surface them.
    accounts: list[SocialAccount] = field(default_factory=list)
    identity_clusters: list[IdentityCluster] = field(default_factory=list)
    relationships: list[Relationship] = field(default_factory=list)
    merges: list[MergeProposal] = field(default_factory=list)
    # Phase C: per-edge tier + reason for hop explanations.
    edge_tier: dict[tuple[str, str], str] = field(default_factory=dict)
    edge_reason: dict[tuple[str, str], str] = field(default_factory=dict)

    def tier(self, a: str, b: str) -> str:
        key = (a, b) if a < b else (b, a)
        return self.edge_tier.get(key, "direct")

    def reason(self, a: str, b: str) -> str:
        key = (a, b) if a < b else (b, a)
        return self.edge_reason.get(key, "direct connection")

    def strength(self, a: str, b: str) -> float:
        key = (a, b) if a < b else (b, a)
        return self.edge_strength.get(key, DEFAULT_STRENGTH)

    def edge_cost(self, a: str, b: str) -> float:
        s = self.strength(a, b)
        return 1.0 / s if s > 0 else float("inf")

    def label(self, node_id: str) -> str:
        name = self.id_to_name.get(node_id)
        return f"{name} ({node_id})" if name and name != node_id else node_id

    def detailed_label(self, node_id: str) -> str:
        base = self.label(node_id)
        meta = self.id_to_meta.get(node_id, {})
        company = meta.get("company", "")
        team = meta.get("team", "")
        if company and team.startswith(f"{company} / "):
            team = team[len(company) + 3:]
        display = {"company": company, "team": team, "role": meta.get("role", "")}
        parts = [display[k] for k in META_FIELDS if display[k]]
        return f"{base} [{' / '.join(parts)}]" if parts else base


def build_graph_from_repository(
    repo: GraphRepository,
    resolver: IdentityResolver | None = None,
) -> Graph:
    """Construct the runtime Graph from any GraphRepository.

    This is the future-facing entry point: pass a TwitterRepository,
    LinkedInRepository, MultiSourceRepository, etc. The pathfinder is
    agnostic to where the data came from.
    """
    payload = repo.load()
    return _build_graph_from_payload(payload, resolver)


def build_graph(
    people_path: str,
    edges_path: str,
    identities_path: str | None = None,
    resolver: IdentityResolver | None = None,
    derive_shared_org: bool = False,
    shared_org_group_by: str = "team",
) -> Graph:
    """Convenience: construct a Graph from CSV files.

    Backwards compatible with the original two-arg signature. Pass
    `identities_path` to also ingest social-account mappings via
    CSVRepository. `resolver` defaults to `SharedAccountResolver`,
    which collapses persons that share a platform account (Phase B);
    pass `ManualCSVResolver()` to disable merging.

    Phase C: pass `derive_shared_org=True` to synthesize person-person
    edges of tier "shared_org" (strength 5) between every pair of
    people sharing a `shared_org_group_by` attribute (default "team").
    Existing direct edges win on dedup since they have higher strength.
    """
    repo = CSVRepository(
        people_path=people_path,
        edges_path=edges_path,
        identities_path=identities_path,
    )
    payload = repo.load()
    if derive_shared_org:
        derived = derive_shared_org_relationships(
            payload.people, group_by=shared_org_group_by
        )
        if derived:
            print(
                f"info: derived {len(derived)} shared_org edge(s) "
                f"grouped by '{shared_org_group_by}'.",
                file=sys.stderr,
            )
            payload = RepositoryPayload(
                people=payload.people,
                accounts=payload.accounts,
                relationships=list(payload.relationships) + derived,
                account_claims=payload.account_claims,
            )
    return _build_graph_from_payload(payload, resolver or SharedAccountResolver())


def _build_graph_from_payload(
    payload: RepositoryPayload,
    resolver: IdentityResolver | None,
) -> Graph:
    # Phase B: ask the resolver for merge proposals, apply them to the
    # payload, then rebuild clusters from the merged data. Routing then
    # operates on the canonical person ids only.
    merges: list[MergeProposal] = []
    if resolver is not None:
        first_pass = resolver.resolve(payload)
        merges = list(first_pass.merges)
        if merges:
            payload = apply_merges(payload, merges)
            for m in merges:
                print(
                    f"info: merging {m.merged_ids} into {m.canonical_id} "
                    f"({m.reason}, confidence={m.confidence:.2f})",
                    file=sys.stderr,
                )
        clusters = list(resolver.resolve(payload).clusters)
    else:
        clusters = []

    id_to_name, name_to_ids, id_to_meta = people_to_lookups(payload.people)
    known_ids = set(id_to_name)

    adjacency: dict[str, set[str]] = defaultdict(set)
    edge_strength: dict[tuple[str, str], float] = {}
    edge_tier: dict[tuple[str, str], str] = {}
    edge_reason: dict[tuple[str, str], str] = {}
    unknown: set[str] = set()
    saw_explicit_strength = False
    for rel in payload.relationships:
        # Phase A: only person-person edges feed routing. Future phases
        # may translate person-account / account-account into routable
        # edges; for now we just stash them on the Graph for output.
        if rel.kind != "person-person":
            continue
        a, b = rel.from_id, rel.to_id
        if a not in known_ids:
            unknown.add(a)
            continue
        if b not in known_ids:
            unknown.add(b)
            continue
        if a == b:
            continue
        adjacency[a].add(b)
        adjacency[b].add(a)
        key = (a, b) if a < b else (b, a)
        s = rel.strength if rel.strength and rel.strength > 0 else DEFAULT_STRENGTH
        # If the edge appears twice keep the stronger, and let its tier
        # win — strongest-evidence wins both routing weight and reason.
        prev = edge_strength.get(key)
        if prev is None or s > prev:
            edge_strength[key] = s
            edge_tier[key] = rel.tier or "direct"
            edge_reason[key] = reason_for(rel)
        if rel.strength and rel.strength != DEFAULT_STRENGTH:
            saw_explicit_strength = True

    if unknown:
        sample = ", ".join(sorted(unknown)[:5])
        print(
            f"warning: {len(unknown)} edge endpoint(s) not found in people "
            f"data (skipped). Examples: {sample}",
            file=sys.stderr,
        )
    if not saw_explicit_strength and adjacency:
        print(
            "warning: no 'strength' column / values in edges; all edges "
            "treated with strength=1.0 (unweighted mode).",
            file=sys.stderr,
        )
    for pid in known_ids:
        adjacency.setdefault(pid, set())

    return Graph(
        adjacency=dict(adjacency),
        id_to_name=id_to_name,
        name_to_ids=name_to_ids,
        id_to_meta=id_to_meta,
        edge_strength=edge_strength,
        accounts=list(payload.accounts),
        identity_clusters=clusters,
        relationships=list(payload.relationships),
        merges=merges,
        edge_tier=edge_tier,
        edge_reason=edge_reason,
    )


def resolve(graph: Graph, token: str) -> str:
    token = token.strip()
    if token in graph.id_to_name:
        return token
    matches = graph.name_to_ids.get(token.lower(), [])
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ValueError(f"unknown person: {token!r}")
    raise ValueError(
        f"ambiguous name {token!r} -> matches ids {matches}; pass id instead"
    )


def multi_source_dijkstra(
    graph: Graph, sources: list[str], target: str
) -> tuple[float, dict[str, set[str]], dict[str, str]] | None:
    """Multi-source Dijkstra recording all min-cost-path predecessors.

    Edge cost = 1 / strength. Returns (min_cost_to_target, predecessors,
    source_of) or None if unreachable. With uniform strength this
    reproduces BFS (hop-count shortest path).
    """
    distance: dict[str, float] = {}
    predecessors: dict[str, set[str]] = defaultdict(set)
    source_of: dict[str, str] = {}
    heap: list[tuple[float, int, str]] = []
    counter = 0
    for src in sources:
        if src not in distance:
            distance[src] = 0.0
            source_of[src] = src
            heapq.heappush(heap, (0.0, counter, src))
            counter += 1

    while heap:
        d, _, node = heapq.heappop(heap)
        if d > distance.get(node, float("inf")) + EDGE_DIST_TOL:
            continue
        if node == target:
            continue  # don't expand past target
        for nb in graph.adjacency.get(node, ()):
            nd = d + graph.edge_cost(node, nb)
            prev_d = distance.get(nb)
            if prev_d is None or nd < prev_d - EDGE_DIST_TOL:
                distance[nb] = nd
                source_of[nb] = source_of[node]
                predecessors[nb] = {node}
                heapq.heappush(heap, (nd, counter, nb))
                counter += 1
            elif math.isclose(nd, prev_d, abs_tol=EDGE_DIST_TOL):
                predecessors[nb].add(node)

    if target not in distance:
        return None
    return distance[target], predecessors, source_of


def enumerate_paths(
    target: str,
    sources: set[str],
    predecessors: dict[str, set[str]],
    limit: int,
) -> list[list[str]]:
    """Enumerate up to `limit` shortest paths from any source to target."""
    paths: list[list[str]] = []

    def walk(node: str, suffix: list[str]) -> None:
        if len(paths) >= limit:
            return
        path = [node, *suffix]
        if node in sources:
            paths.append(path)
            return
        for prev in sorted(predecessors.get(node, ())):
            walk(prev, path)
            if len(paths) >= limit:
                return

    walk(target, [])
    return paths


def _fmt_strength(s: float) -> str:
    return str(int(s)) if float(s).is_integer() else f"{s:.2f}"


def format_path(graph: Graph, path: list[str], explain: bool = False) -> str:
    labeler = graph.detailed_label if explain else graph.label
    if not path:
        return ""
    if len(path) == 1:
        return labeler(path[0])
    parts = [labeler(path[0])]
    for i in range(1, len(path)):
        s = graph.strength(path[i - 1], path[i])
        tier = graph.tier(path[i - 1], path[i])
        arrow_label = f"{_fmt_strength(s)}|{tier}"
        if explain:
            reason = graph.reason(path[i - 1], path[i])
            parts.append(f"\n    -({arrow_label}: {reason})-> ")
        else:
            parts.append(f" -({arrow_label})-> ")
        parts.append(labeler(path[i]))
    return "".join(parts)


def path_total_strength(graph: Graph, path: list[str]) -> float:
    return sum(graph.strength(path[i - 1], path[i]) for i in range(1, len(path)))


def path_total_cost(graph: Graph, path: list[str]) -> float:
    return sum(graph.edge_cost(path[i - 1], path[i]) for i in range(1, len(path)))


def find_warm_intro(
    graph: Graph, entries: list[str], target: str, top_k: int
) -> dict:
    entry_ids = [resolve(graph, e) for e in entries]
    target_id = resolve(graph, target)

    if target_id in entry_ids:
        return {
            "best": [target_id],
            "hops": 0,
            "cost": 0.0,
            "total_strength": 0.0,
            "entry_used": target_id,
            "alternatives": [],
            "explanation": "Target is itself an entry point; no introduction needed.",
        }

    result = multi_source_dijkstra(graph, entry_ids, target_id)
    if result is None:
        return {
            "best": None,
            "hops": None,
            "cost": None,
            "total_strength": None,
            "entry_used": None,
            "alternatives": [],
            "explanation": (
                f"No path exists from any entry point to {graph.label(target_id)} "
                f"in the provided graph."
            ),
        }

    cost, predecessors, _source_of = result
    paths = enumerate_paths(target_id, set(entry_ids), predecessors, top_k)
    best = paths[0]
    entry_used = best[0]
    hops = len(best) - 1
    total_strength = path_total_strength(graph, best)
    explanation = (
        f"Best warm path has {hops} hop(s) with total strength "
        f"{_fmt_strength(total_strength)} (cost {cost:.3f}), routed through entry "
        f"point {graph.label(entry_used)}. Selected via multi-source Dijkstra "
        f"on an undirected graph with edge cost = 1/strength; "
        f"{len(paths) - 1} alternative path(s) of the same total cost were found."
    )
    return {
        "best": best,
        "hops": hops,
        "cost": cost,
        "total_strength": total_strength,
        "entry_used": entry_used,
        "alternatives": paths[1:],
        "explanation": explanation,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Find a warm introduction path.")
    parser.add_argument("--people", required=True, help="Path to people.csv (id,name)")
    parser.add_argument("--edges", required=True, help="Path to edges.csv (from,to)")
    parser.add_argument(
        "--identities",
        help="Optional path to identities.csv (person_id,platform,handle). "
        "Enables Phase B identity merging via shared accounts.",
    )
    parser.add_argument(
        "--derive-shared-org",
        action="store_true",
        help="Phase C: synthesize shared_org edges (strength 5) between "
        "every pair of people sharing the same `team` attribute.",
    )
    parser.add_argument(
        "--entry",
        required=True,
        help="Comma-separated entry-point ids or names (one or more)",
    )
    parser.add_argument("--target", required=True, help="Target person id or name")
    parser.add_argument(
        "--top-k", type=int, default=3, help="Max number of shortest paths to show"
    )
    parser.add_argument(
        "--output",
        help="Optional path to write the full result as JSON. Default is "
        "the Phase D structured shape (nodes/edges/identity_clusters/paths). "
        "Use --legacy-output for the pre-Phase-D flat shape.",
    )
    parser.add_argument(
        "--legacy-output",
        action="store_true",
        help="Write the legacy flat JSON shape (best_path/alternatives) "
        "instead of the new structured shape.",
    )
    parser.add_argument(
        "--explain",
        action="store_true",
        help="Show company, team, and role alongside each person in the output",
    )
    args = parser.parse_args(argv)

    graph = build_graph(
        args.people,
        args.edges,
        identities_path=args.identities,
        derive_shared_org=args.derive_shared_org,
    )
    entries = [e for e in args.entry.split(",") if e.strip()]
    if not entries:
        parser.error("--entry must contain at least one person")

    try:
        result = find_warm_intro(graph, entries, args.target, args.top_k)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.output:
        if args.legacy_output:
            write_result_json(args.output, graph, result, explain=args.explain)
        else:
            write_result_json_v2(
                args.output,
                graph,
                result,
                entries=entries,
                target=args.target,
                top_k=args.top_k,
            )
        print(f"Wrote {args.output}", file=sys.stderr)

    if result["best"] is None:
        print(result["explanation"])
        return 1

    label = graph.detailed_label if args.explain else graph.label
    ts = _fmt_strength(result["total_strength"])
    print(
        f"Best warm intro path ({result['hops']} hop(s), total strength {ts}):"
    )
    print(f"  {format_path(graph, result['best'], explain=args.explain)}")
    print(f"Entry point used: {label(result['entry_used'])}")
    if result["alternatives"]:
        print(f"\nAlternative paths ({len(result['alternatives'])}):")
        for p in result["alternatives"]:
            alt_ts = _fmt_strength(path_total_strength(graph, p))
            print(
                f"  [{len(p) - 1} hops, strength {alt_ts}] "
                f"{format_path(graph, p, explain=args.explain)}"
            )
    print(f"\nWhy: {result['explanation']}")
    return 0


def _path_payload(graph: Graph, path: list[str], explain: bool = False) -> dict:
    nodes = []
    for p in path:
        node = {"id": p, "name": graph.id_to_name.get(p, p)}
        if explain:
            node.update(graph.id_to_meta.get(p, {}))
        nodes.append(node)
    edges = [
        {
            "from": path[i - 1],
            "to": path[i],
            "strength": graph.strength(path[i - 1], path[i]),
            "cost": graph.edge_cost(path[i - 1], path[i]),
            "tier": graph.tier(path[i - 1], path[i]),
            "reason": graph.reason(path[i - 1], path[i]),
        }
        for i in range(1, len(path))
    ]
    return {
        "ids": path,
        "nodes": nodes,
        "edges": edges,
        "hops": max(len(path) - 1, 0),
        "total_strength": path_total_strength(graph, path),
        "total_cost": path_total_cost(graph, path),
        "display": format_path(graph, path, explain=explain),
    }


def _account_payload(graph: Graph, account_id: str) -> dict:
    for acc in graph.accounts:
        if acc.id == account_id:
            return {
                "id": acc.id,
                "platform": acc.platform,
                "handle": acc.handle,
            }
    return {"id": account_id, "platform": "", "handle": ""}


def _identity_clusters_payload(graph: Graph) -> list[dict]:
    return [
        {
            "person_id": c.person_id,
            "person_name": graph.id_to_name.get(c.person_id, c.person_id),
            "account_count": len(c.account_ids),
            "accounts": [_account_payload(graph, a) for a in c.account_ids],
        }
        for c in graph.identity_clusters
    ]


def _merges_payload(graph: Graph) -> list[dict]:
    return [
        {
            "canonical_id": m.canonical_id,
            "canonical_name": graph.id_to_name.get(m.canonical_id, m.canonical_id),
            "merged_ids": list(m.merged_ids),
            "reason": m.reason,
            "confidence": m.confidence,
        }
        for m in graph.merges
    ]


def write_result_json(
    path: str, graph: Graph, result: dict, explain: bool = False
) -> None:
    reachable = result["best"] is not None
    entry_id = result["entry_used"]
    entry_used = None
    if entry_id is not None:
        entry_used = {"id": entry_id, "name": graph.id_to_name.get(entry_id, entry_id)}
        if explain:
            entry_used.update(graph.id_to_meta.get(entry_id, {}))
    payload = {
        "reachable": reachable,
        "hops": result["hops"],
        "cost": result.get("cost"),
        "total_strength": result.get("total_strength"),
        "entry_used": entry_used,
        "best_path": (
            _path_payload(graph, result["best"], explain=explain) if reachable else None
        ),
        "alternatives": [
            _path_payload(graph, p, explain=explain) for p in result["alternatives"]
        ],
        "explanation": result["explanation"],
        "identity_clusters": _identity_clusters_payload(graph),
        "merges": _merges_payload(graph),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


# --- Phase D: structured output (nodes/edges/identity_clusters/paths) -----


def _confidence_for(strength: float | None) -> float:
    """Map an edge strength onto a [0, 1] confidence score.

    Mapping mirrors TIER_STRENGTH so direct=10 -> 1.0, mutual=8 -> 0.8,
    shared_org=5 -> 0.5, platform_similarity=2 -> 0.2. Heuristic
    resolvers can override by emitting their own confidence in
    Relationship.attributes['confidence'] later.
    """
    if not strength or strength <= 0:
        return 0.0
    return min(strength / 10.0, 1.0)


def _node_views(graph: Graph) -> list[dict]:
    """Unified node list: every Person + every SocialAccount."""
    nodes: list[dict] = []
    for pid, name in graph.id_to_name.items():
        meta = graph.id_to_meta.get(pid, {})
        nodes.append(
            {
                "id": pid,
                "type": "person",
                "name": name,
                "attributes": {k: v for k, v in meta.items() if v},
            }
        )
    for acc in graph.accounts:
        nodes.append(
            {
                "id": acc.id,
                "type": "account",
                "platform": acc.platform,
                "handle": acc.handle,
                "owner_person_id": acc.owner_person_id,
            }
        )
    return nodes


def _edge_views(graph: Graph) -> list[dict]:
    """Unified edge list: every Relationship + ownership edges from accounts."""
    edges: list[dict] = []
    for rel in graph.relationships:
        edges.append(
            {
                "from_id": rel.from_id,
                "to_id": rel.to_id,
                "kind": rel.kind,
                "tier": rel.tier,
                "strength": rel.strength,
                "cost": (1.0 / rel.strength) if rel.strength and rel.strength > 0 else None,
                "reason": reason_for(rel),
                "confidence": _confidence_for(rel.strength),
                "source": rel.source,
            }
        )
    # Person <-> Account ownership: implicit via SocialAccount.owner_person_id.
    # Surface as edges so consumers see the full graph.
    for acc in graph.accounts:
        if acc.owner_person_id:
            edges.append(
                {
                    "from_id": acc.owner_person_id,
                    "to_id": acc.id,
                    "kind": "person-account",
                    "tier": None,
                    "strength": None,
                    "cost": None,
                    "reason": f"owns {acc.platform} account",
                    "confidence": 1.0,
                    "source": "csv",
                }
            )
    return edges


def _hops_for_path(graph: Graph, path: list[str]) -> list[dict]:
    hops: list[dict] = []
    for i in range(1, len(path)):
        a, b = path[i - 1], path[i]
        s = graph.strength(a, b)
        hops.append(
            {
                "step": i,
                "from_id": a,
                "to_id": b,
                "from_name": graph.id_to_name.get(a, a),
                "to_name": graph.id_to_name.get(b, b),
                "tier": graph.tier(a, b),
                "strength": s,
                "cost": graph.edge_cost(a, b),
                "explanation": graph.reason(a, b),
                "confidence": _confidence_for(s),
            }
        )
    return hops


def _paths_views(graph: Graph, result: dict) -> list[dict]:
    if result.get("best") is None:
        return []
    all_paths = [result["best"], *result.get("alternatives", [])]
    out: list[dict] = []
    for rank, p in enumerate(all_paths, start=1):
        out.append(
            {
                "rank": rank,
                "is_best": rank == 1,
                "entry_used": p[0],
                "target": p[-1],
                "hops_count": max(len(p) - 1, 0),
                "total_strength": path_total_strength(graph, p),
                "total_cost": path_total_cost(graph, p),
                "hops": _hops_for_path(graph, p),
            }
        )
    return out


def write_result_json_v2(
    path: str,
    graph: Graph,
    result: dict,
    entries: list[str] | None = None,
    target: str | None = None,
    top_k: int | None = None,
) -> None:
    """Phase D structured shape: nodes, edges, identity_clusters, paths."""
    nodes = _node_views(graph)
    edges = _edge_views(graph)
    payload = {
        "schema_version": 2,
        "query": {
            "entries": list(entries or []),
            "target": target,
            "top_k": top_k,
        },
        "summary": {
            "reachable": result.get("best") is not None,
            "people_count": len(graph.id_to_name),
            "account_count": len(graph.accounts),
            "person_edge_count": sum(len(v) for v in graph.adjacency.values()) // 2,
            "edge_count": len(edges),
            "identity_cluster_count": len(graph.identity_clusters),
            "merge_count": len(graph.merges),
            "path_count": len(result.get("alternatives", [])) + (
                1 if result.get("best") else 0
            ),
            "best_hops": result.get("hops"),
            "best_total_strength": result.get("total_strength"),
            "best_cost": result.get("cost"),
        },
        "nodes": nodes,
        "edges": edges,
        "identity_clusters": _identity_clusters_payload(graph),
        "merges": _merges_payload(graph),
        "paths": _paths_views(graph, result),
        "explanation": result.get("explanation"),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    raise SystemExit(main())
