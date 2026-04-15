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
import csv
import json
import sys
import heapq
import math
from collections import defaultdict, deque
from dataclasses import dataclass, field


META_FIELDS = ("company", "team", "role")


DEFAULT_STRENGTH = 1.0
EDGE_DIST_TOL = 1e-9


@dataclass
class Graph:
    adjacency: dict[str, set[str]]
    id_to_name: dict[str, str]
    name_to_ids: dict[str, list[str]]
    id_to_meta: dict[str, dict[str, str]]
    edge_strength: dict[tuple[str, str], float] = field(default_factory=dict)

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


def load_people(
    path: str,
) -> tuple[dict[str, str], dict[str, list[str]], dict[str, dict[str, str]]]:
    id_to_name: dict[str, str] = {}
    name_to_ids: dict[str, list[str]] = defaultdict(list)
    id_to_meta: dict[str, dict[str, str]] = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "id" not in reader.fieldnames:
            raise ValueError(f"{path}: expected header with at least 'id' column")
        has_name = "name" in reader.fieldnames
        for row in reader:
            pid = (row["id"] or "").strip()
            if not pid:
                continue
            name = (row["name"].strip() if has_name and row["name"] else pid)
            if pid in id_to_name:
                raise ValueError(f"{path}: duplicate id {pid!r}")
            id_to_name[pid] = name
            name_to_ids[name.lower()].append(pid)
            id_to_meta[pid] = {
                k: (row[k].strip() if row.get(k) else "") for k in META_FIELDS
            }
    return id_to_name, dict(name_to_ids), id_to_meta


def load_edges(
    path: str, known_ids: set[str]
) -> tuple[dict[str, set[str]], dict[tuple[str, str], float]]:
    adjacency: dict[str, set[str]] = defaultdict(set)
    strengths: dict[tuple[str, str], float] = {}
    unknown: set[str] = set()
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or {"from", "to"} - set(reader.fieldnames):
            raise ValueError(f"{path}: expected header with 'from' and 'to' columns")
        has_strength = "strength" in reader.fieldnames
        for row in reader:
            a = (row["from"] or "").strip()
            b = (row["to"] or "").strip()
            if not a or not b or a == b:
                continue
            if a not in known_ids:
                unknown.add(a)
                continue
            if b not in known_ids:
                unknown.add(b)
                continue
            adjacency[a].add(b)
            adjacency[b].add(a)
            if has_strength:
                raw = (row.get("strength") or "").strip()
                try:
                    s = float(raw) if raw else DEFAULT_STRENGTH
                except ValueError:
                    s = DEFAULT_STRENGTH
                if s <= 0:
                    s = DEFAULT_STRENGTH
                key = (a, b) if a < b else (b, a)
                # If the edge appears twice with different strengths, keep the stronger.
                strengths[key] = max(strengths.get(key, s), s)
    if unknown:
        sample = ", ".join(sorted(unknown)[:5])
        print(
            f"warning: {len(unknown)} edge endpoint(s) not found in people.csv "
            f"(skipped). Examples: {sample}",
            file=sys.stderr,
        )
    if not has_strength:
        print(
            "warning: edges.csv has no 'strength' column; all edges treated "
            "with strength=1.0 (unweighted mode).",
            file=sys.stderr,
        )
    for pid in known_ids:
        adjacency.setdefault(pid, set())
    return dict(adjacency), strengths


def build_graph(people_path: str, edges_path: str) -> Graph:
    id_to_name, name_to_ids, id_to_meta = load_people(people_path)
    adjacency, edge_strength = load_edges(edges_path, set(id_to_name))
    return Graph(
        adjacency=adjacency,
        id_to_name=id_to_name,
        name_to_ids=name_to_ids,
        id_to_meta=id_to_meta,
        edge_strength=edge_strength,
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
        arrow = f" -({_fmt_strength(s)})-> "
        if explain:
            arrow = f"\n    -({_fmt_strength(s)})-> "
        parts.append(arrow)
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
        help="Optional path to write the full result as JSON (paths enriched with names)",
    )
    parser.add_argument(
        "--explain",
        action="store_true",
        help="Show company, team, and role alongside each person in the output",
    )
    args = parser.parse_args(argv)

    graph = build_graph(args.people, args.edges)
    entries = [e for e in args.entry.split(",") if e.strip()]
    if not entries:
        parser.error("--entry must contain at least one person")

    try:
        result = find_warm_intro(graph, entries, args.target, args.top_k)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.output:
        write_result_json(args.output, graph, result, explain=args.explain)
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
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    raise SystemExit(main())
