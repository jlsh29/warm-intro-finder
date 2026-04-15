"""Stress tests for the warm-intro path finder.

Exercises:
  - Disconnected components (target unreachable from chosen entries)
  - Multiple shortest paths of equal length
  - Cycles (triangles, 6-cycle, self-loop)
  - Multi-entry routing (best entry chosen automatically)
  - Duplicate edges and unknown endpoints (deduped / skipped with warning)
  - Ambiguous names (must error out, not guess)
  - Entry == target (zero hops)

Run:  python run_stress_tests.py
Exits non-zero if any case fails its assertion.
"""

from __future__ import annotations

import sys

from warm_intro import build_graph, find_warm_intro, resolve

PEOPLE = "test_people.csv"
EDGES = "test_edges.csv"


def header(title: str) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")


def show(result: dict, graph) -> None:
    if result["best"] is None:
        print(f"  -> UNREACHABLE: {result['explanation']}")
        return
    path = " -> ".join(graph.label(p) for p in result["best"])
    print(f"  hops={result['hops']}  entry={graph.label(result['entry_used'])}")
    print(f"  best: {path}")
    for alt in result["alternatives"]:
        print(f"  alt:  {' -> '.join(graph.label(p) for p in alt)}")


def main() -> int:
    graph = build_graph(PEOPLE, EDGES)
    failures: list[str] = []

    def check(name: str, cond: bool, detail: str = "") -> None:
        if not cond:
            failures.append(f"{name}: {detail}")
            print(f"  FAIL: {detail}")
        else:
            print("  OK")

    # ---- 1. Multi-entry, many shortest paths through hub ------------------
    header("1. Multi-entry BFS — many shortest paths through hub Ivan")
    r = find_warm_intro(graph, ["Alice", "Bob", "Carol"], "Zoe", top_k=10)
    show(r, graph)
    check("multi-entry hops", r["hops"] == 5, f"expected 5, got {r['hops']}")
    check(
        "multi-entry alternatives",
        len(r["alternatives"]) >= 4,
        f"expected >=4 alts, got {len(r['alternatives'])}",
    )
    check(
        "entry is one of the supplied entries",
        r["entry_used"] in {resolve(graph, x) for x in ("Alice", "Bob", "Carol")},
        f"entry was {r['entry_used']}",
    )
    # Ivan (n09) must appear in every shortest path — he's the only bridge.
    all_paths = [r["best"], *r["alternatives"]]
    check(
        "hub Ivan appears in every shortest path",
        all("n09" in p for p in all_paths),
        "Ivan missing from at least one shortest path",
    )

    # ---- 2. Disconnected component — target unreachable -------------------
    header("2. Disconnected — entry IslandA1, target Zoe")
    r = find_warm_intro(graph, ["IslandA1"], "Zoe", top_k=3)
    show(r, graph)
    check("disconnected unreachable", r["best"] is None, "expected no path")

    # ---- 3. Disconnected — but target reachable via OTHER entry ----------
    header("3. Mixed entries — IslandA1 isolated but Alice can reach Zoe")
    r = find_warm_intro(graph, ["IslandA1", "Alice"], "Zoe", top_k=3)
    show(r, graph)
    check(
        "falls back to reachable entry",
        r["best"] is not None and r["entry_used"] == "n01",
        "should route via Alice, ignoring isolated entry",
    )

    # ---- 4. Triangle / cycle handling — must terminate -------------------
    header("4. Cycle handling — Dave -> Eve (triangle n04-n05-n06)")
    r = find_warm_intro(graph, ["Dave"], "Eve", top_k=5)
    show(r, graph)
    check("triangle 1 hop", r["hops"] == 1, f"expected 1, got {r['hops']}")

    # ---- 5. Six-cycle around Ivan (n09->n20->...->n25->n09) --------------
    header("5. Six-cycle — Alice to Uma via Ivan")
    r = find_warm_intro(graph, ["Alice"], "Uma", top_k=5)
    show(r, graph)
    check("six-cycle reachable", r["best"] is not None, "Uma should be reachable")

    # ---- 6. Long alternative path NOT surfaced as shortest --------------
    header("6. Long alternative — Alice -> Zoe (chain via n13..n18 is longer)")
    r = find_warm_intro(graph, ["Alice"], "Zoe", top_k=10)
    show(r, graph)
    # Long chain is Alice->n13->n14->n16->n17->n18->Zoe = 6 hops
    # Short path through hub = 5 hops. None of the shortest should include n13.
    check(
        "long chain excluded from shortest set",
        all("n13" not in p for p in [r["best"], *r["alternatives"]]),
        "long chain leaked into shortest results",
    )

    # ---- 7. Entry == target (zero hops) ---------------------------------
    header("7. Entry == target")
    r = find_warm_intro(graph, ["Zoe"], "Zoe", top_k=3)
    show(r, graph)
    check("zero hops", r["hops"] == 0, f"expected 0, got {r['hops']}")

    # ---- 8. Ambiguous name must error -----------------------------------
    header("8. Ambiguous name 'Echo' must raise, not guess")
    try:
        find_warm_intro(graph, ["Alice"], "Echo", top_k=1)
        check("ambiguous raises", False, "no error raised for ambiguous name")
    except ValueError as e:
        print(f"  raised: {e}")
        check("ambiguous raises", True)

    # ---- 9. Disambiguation by id works ----------------------------------
    header("9. Disambiguation by id — Echo n31 -> Echo n32")
    r = find_warm_intro(graph, ["n31"], "n32", top_k=1)
    show(r, graph)
    check("echo pair 1 hop", r["hops"] == 1, f"expected 1, got {r['hops']}")

    # ---- 10. Self-loop (n19,n19) ignored — node still isolated ----------
    header("10. Self-loop ignored — n19 isolated, can't reach anything")
    r = find_warm_intro(graph, ["n19"], "Alice", top_k=1)
    show(r, graph)
    check("self-loop yields isolation", r["best"] is None, "n19 should be isolated")

    # ---- 11. Duplicate edges deduped — Judy<->Zoe still 1 hop -----------
    header("11. Duplicate edge dedup — Judy -> Zoe")
    n_zoe_neighbors = len(graph.adjacency["n10"])
    print(f"  Judy (n10) neighbor count = {n_zoe_neighbors}")
    # Judy connects to: Ivan(n09), Karl(n11)? no. Zoe(n26). And the dup row.
    # Expect Ivan + Zoe = 2 (duplicate n10->n26 must collapse).
    check(
        "duplicates collapsed",
        n_zoe_neighbors == 2,
        f"expected 2 unique neighbors for Judy, got {n_zoe_neighbors}",
    )

    # ---- 12. Top-k caps results ----------------------------------------
    header("12. top_k limits results")
    r2 = find_warm_intro(graph, ["Alice", "Bob", "Carol"], "Zoe", top_k=2)
    show(r2, graph)
    check(
        "top_k=2 -> 1 best + <=1 alt",
        len(r2["alternatives"]) <= 1,
        f"got {len(r2['alternatives'])} alts",
    )

    # ---- summary --------------------------------------------------------
    header("SUMMARY")
    if failures:
        print(f"FAILED ({len(failures)}):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("All stress tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
