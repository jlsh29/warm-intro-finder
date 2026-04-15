"""Analyze people.csv + edges.csv for connector/introducer signals.

Prints a report to stdout and also writes:
  - network_report.json   (machine-readable)
  - network_report.txt    (human-readable, same content as stdout)
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict, deque


def load() -> tuple[dict[str, dict], dict[str, set[str]]]:
    people: dict[str, dict] = {}
    with open("people.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            people[row["id"]] = row
    adj: dict[str, set[str]] = defaultdict(set)
    with open("edges.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            a, b = row["from"], row["to"]
            if a == b or a not in people or b not in people:
                continue
            adj[a].add(b)
            adj[b].add(a)
    for pid in people:
        adj.setdefault(pid, set())
    return people, adj


def betweenness(people: dict[str, dict], adj: dict[str, set[str]]) -> dict[str, float]:
    """Brandes' algorithm — unweighted betweenness centrality."""
    cb: dict[str, float] = {v: 0.0 for v in people}
    for s in people:
        stack: list[str] = []
        preds: dict[str, list[str]] = {v: [] for v in people}
        sigma: dict[str, int] = {v: 0 for v in people}
        sigma[s] = 1
        dist: dict[str, int] = {v: -1 for v in people}
        dist[s] = 0
        q: deque[str] = deque([s])
        while q:
            v = q.popleft()
            stack.append(v)
            for w in adj[v]:
                if dist[w] < 0:
                    dist[w] = dist[v] + 1
                    q.append(w)
                if dist[w] == dist[v] + 1:
                    sigma[w] += sigma[v]
                    preds[w].append(v)
        delta: dict[str, float] = {v: 0.0 for v in people}
        while stack:
            w = stack.pop()
            for v in preds[w]:
                delta[v] += (sigma[v] / sigma[w]) * (1 + delta[w])
            if w != s:
                cb[w] += delta[w]
    # undirected: each pair counted twice
    return {v: c / 2 for v, c in cb.items()}


def build_report(people: dict[str, dict], adj: dict[str, set[str]]) -> dict:
    by_id = people

    deg = {pid: len(nbrs) for pid, nbrs in adj.items()}
    top_deg = sorted(deg.items(), key=lambda x: -x[1])[:10]

    # Cross-company bridges
    cross_count: dict[str, int] = defaultdict(int)
    bridge_people: dict[str, set[str]] = defaultdict(set)
    for a, nbrs in adj.items():
        for b in nbrs:
            if a < b and by_id[a]["company"] != by_id[b]["company"]:
                cross_count[by_id[a]["company"]] += 1
                cross_count[by_id[b]["company"]] += 1
                bridge_people[by_id[a]["company"]].add(a)
                bridge_people[by_id[b]["company"]].add(b)
    company_sizes: dict[str, int] = defaultdict(int)
    for p in by_id.values():
        company_sizes[p["company"]] += 1
    bridge_rows = sorted(
        cross_count.items(), key=lambda x: (-x[1], -len(bridge_people[x[0]]))
    )

    # Betweenness + composite introducer score
    bc = betweenness(people, adj)
    n = len(people)
    max_bc = (n - 1) * (n - 2) / 2
    bc_norm = {v: c / max_bc for v, c in bc.items()} if max_bc else bc

    cross_neighbors: dict[str, int] = {}
    for pid, nbrs in adj.items():
        my_co = by_id[pid]["company"]
        cross_neighbors[pid] = sum(
            1 for nb in nbrs if by_id[nb]["company"] != my_co
        )

    max_deg = max(deg.values()) or 1
    max_cross = max(cross_neighbors.values()) or 1
    max_bcn = max(bc_norm.values()) or 1
    score = {
        pid: 0.60 * (bc_norm[pid] / max_bcn)
        + 0.25 * (deg[pid] / max_deg)
        + 0.15 * (cross_neighbors[pid] / max_cross)
        for pid in people
    }
    top_intro = sorted(score.items(), key=lambda x: -x[1])[:10]

    return {
        "summary": {
            "people": n,
            "edges": sum(len(v) for v in adj.values()) // 2,
            "companies": len(company_sizes),
            "scoring_weights": {
                "betweenness": 0.60,
                "degree": 0.25,
                "cross_company_neighbors": 0.15,
            },
        },
        "top_connected": [
            {
                "rank": i,
                "id": pid,
                "name": by_id[pid]["name"],
                "company": by_id[pid]["company"],
                "role": by_id[pid]["role"],
                "connections": d,
            }
            for i, (pid, d) in enumerate(top_deg, 1)
        ],
        "bridges_by_company": [
            {
                "company": c,
                "bridge_edges": n_edges,
                "bridge_people": len(bridge_people[c]),
                "company_size": company_sizes[c],
            }
            for c, n_edges in bridge_rows
        ],
        "top_introducers": [
            {
                "rank": i,
                "id": pid,
                "name": by_id[pid]["name"],
                "company": by_id[pid]["company"],
                "role": by_id[pid]["role"],
                "betweenness": round(bc_norm[pid], 4),
                "degree": deg[pid],
                "cross_company_neighbors": cross_neighbors[pid],
                "score": round(s, 4),
            }
            for i, (pid, s) in enumerate(top_intro, 1)
        ],
    }


def format_text(report: dict) -> str:
    lines: list[str] = []

    def add(s: str = "") -> None:
        lines.append(s)

    s = report["summary"]
    add("=" * 72)
    add("NETWORK REPORT")
    add("=" * 72)
    add(
        f"{s['people']} people, {s['edges']} edges, "
        f"{s['companies']} companies"
    )
    w = s["scoring_weights"]
    add(
        f"Introducer score weights: betweenness {w['betweenness']:.2f}, "
        f"degree {w['degree']:.2f}, cross-co {w['cross_company_neighbors']:.2f}"
    )

    add("")
    add("=" * 72)
    add("TOP 10 MOST CONNECTED PEOPLE (by degree)")
    add("=" * 72)
    add(f"{'Rank':<5}{'Name':<22}{'Company':<22}{'Role':<22}{'Conns':>5}")
    add("-" * 72)
    for r in report["top_connected"]:
        add(
            f"{r['rank']:<5}{r['name']:<22}{r['company']:<22}"
            f"{r['role']:<22}{r['connections']:>5}"
        )

    add("")
    add("=" * 72)
    add("CROSS-COMPANY BRIDGES BY COMPANY")
    add("=" * 72)
    add(f"{'Company':<22}{'Bridge edges':>14}{'Bridge people':>16}{'Size':>7}")
    add("-" * 72)
    for r in report["bridges_by_company"]:
        add(
            f"{r['company']:<22}{r['bridge_edges']:>14}"
            f"{r['bridge_people']:>16}{r['company_size']:>7}"
        )

    add("")
    add("=" * 72)
    add("TOP 10 INTRODUCERS (composite: betweenness + degree + cross-co reach)")
    add("=" * 72)
    add(
        f"{'Rank':<5}{'Name':<22}{'Company':<20}"
        f"{'Btw':>7}{'Deg':>5}{'X-co':>6}{'Score':>7}"
    )
    add("-" * 72)
    for r in report["top_introducers"]:
        add(
            f"{r['rank']:<5}{r['name']:<22}{r['company']:<20}"
            f"{r['betweenness']:>7.3f}{r['degree']:>5}"
            f"{r['cross_company_neighbors']:>6}{r['score']:>7.3f}"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    people, adj = load()
    report = build_report(people, adj)
    text = format_text(report)
    print(text, end="")
    with open("network_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    with open("network_report.txt", "w", encoding="utf-8") as f:
        f.write(text)
    print("\nWrote network_report.json and network_report.txt")


if __name__ == "__main__":
    main()
