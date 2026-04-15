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

import os
import tempfile

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

    # ---- 13. Phase B: shared-account merging -----------------------------
    header("13. Identity merging — two persons sharing a Twitter handle merge")
    with tempfile.TemporaryDirectory() as td:
        people_path = os.path.join(td, "people.csv")
        edges_path = os.path.join(td, "edges.csv")
        identities_path = os.path.join(td, "identities.csv")
        with open(people_path, "w", encoding="utf-8") as f:
            f.write("id,name\nu1,Alpha\nu2,Beta\nu3,Gamma\n")
        with open(edges_path, "w", encoding="utf-8") as f:
            f.write("from,to,strength\nu1,u3,5\nu2,u3,7\n")
        with open(identities_path, "w", encoding="utf-8") as f:
            # u1 and u2 both claim @same — they are the same person.
            f.write("person_id,platform,handle\n"
                    "u1,twitter,@same\n"
                    "u2,twitter,@same\n"
                    "u2,linkedin,beta-li\n")
        g = build_graph(people_path, edges_path, identities_path=identities_path)
        print(f"  merges proposed: {len(g.merges)}")
        for m in g.merges:
            print(f"    {m.canonical_id} <- {m.merged_ids}: {m.reason}")
        print(f"  surviving people: {sorted(g.id_to_name)}")
        print(f"  identity clusters: {len(g.identity_clusters)}")
        for c in g.identity_clusters:
            print(f"    {c.person_id}: {c.account_ids}")
        # u1 < u2 lexicographically, so u1 wins as canonical.
        check(
            "exactly one merge proposed",
            len(g.merges) == 1,
            f"got {len(g.merges)}",
        )
        check(
            "u1 is canonical, u2 merged",
            g.merges and g.merges[0].canonical_id == "u1"
            and g.merges[0].merged_ids == ["u2"],
            "wrong canonical/merged ids",
        )
        check(
            "u2 dropped from people",
            "u2" not in g.id_to_name,
            "u2 should have been merged away",
        )
        check(
            "u1 owns both linkedin and twitter accounts now",
            any(c.person_id == "u1" and "linkedin:beta-li" in c.account_ids
                and "twitter:@same" in c.account_ids
                for c in g.identity_clusters),
            "u1 cluster missing merged accounts",
        )
        # The relationship u2->u3 should now exist as u1->u3 (max strength wins).
        r = find_warm_intro(g, ["u1"], "u3", top_k=1)
        check(
            "u1 reaches u3 in 1 hop",
            r["hops"] == 1,
            f"expected 1 hop, got {r['hops']}",
        )
        check(
            "merged edge keeps max strength (7)",
            r["total_strength"] == 7,
            f"expected total_strength=7, got {r['total_strength']}",
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
