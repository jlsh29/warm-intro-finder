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

import csv
import json
import os
import tempfile

from twitter_ingester import ingest as twitter_ingest
from warm_intro import (
    build_graph,
    find_warm_intro,
    resolve,
    write_result_json,
    write_result_json_v2,
)

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

    # ---- 14. Phase C: tiered scoring + shared-org derivation -------------
    header("14. Tiered scoring — explicit tier column + derived shared_org")
    with tempfile.TemporaryDirectory() as td:
        people_path = os.path.join(td, "people.csv")
        edges_path = os.path.join(td, "edges.csv")
        with open(people_path, "w", encoding="utf-8") as f:
            f.write(
                "id,name,company,team,role\n"
                "x1,X1,Acme,Acme/Alpha,Engineer\n"
                "x2,X2,Acme,Acme/Alpha,Engineer\n"
                "x3,X3,Acme,Acme/Alpha,Engineer\n"
                "x4,X4,Beta,Beta/Solo,Engineer\n"
            )
        # Explicit tier rows mixing direct and platform_similarity.
        with open(edges_path, "w", encoding="utf-8") as f:
            f.write(
                "from,to,tier\n"
                "x1,x4,direct\n"
                "x2,x4,platform_similarity\n"
            )
        # Without --derive-shared-org: only edges in the CSV exist.
        g = build_graph(people_path, edges_path)
        # x1 -> x4 direct (strength 10); x2 -> x4 platform_similarity (strength 2).
        check(
            "direct tier defaults to strength 10",
            g.strength("x1", "x4") == 10,
            f"expected 10, got {g.strength('x1', 'x4')}",
        )
        check(
            "platform_similarity tier defaults to strength 2",
            g.strength("x2", "x4") == 2,
            f"expected 2, got {g.strength('x2', 'x4')}",
        )
        check(
            "edge tier labels exposed on graph",
            g.tier("x1", "x4") == "direct"
            and g.tier("x2", "x4") == "platform_similarity",
            f"got {g.tier('x1', 'x4')} and {g.tier('x2', 'x4')}",
        )
        # x3 has no edges in the CSV, so unreachable from x1.
        r = find_warm_intro(g, ["x1"], "x3", top_k=1)
        check(
            "x3 unreachable without derivation (no edges in CSV)",
            r["best"] is None,
            f"expected unreachable, got {r['best']}",
        )

        # With --derive-shared-org=True, x1/x2/x3 (same team) gain
        # shared_org edges (strength 5) — now x1 can reach x3.
        g2 = build_graph(people_path, edges_path, derive_shared_org=True)
        r2 = find_warm_intro(g2, ["x1"], "x3", top_k=1)
        check(
            "with derivation, x1 reaches x3 via shared_org",
            r2["best"] is not None and r2["total_strength"] == 5,
            f"expected total_strength=5, got {r2.get('total_strength')}",
        )
        check(
            "derived edge tier is shared_org",
            g2.tier("x1", "x3") == "shared_org",
            f"got tier {g2.tier('x1', 'x3')}",
        )
        check(
            "shared_org reason names the team",
            "Acme/Alpha" in g2.reason("x1", "x3"),
            f"reason was {g2.reason('x1', 'x3')!r}",
        )
        # x1<->x4 explicit direct (strength 10) beats potential derived
        # shared_org (strength 5) but anyway x4 is on a different team,
        # so no derived edge exists there.
        check(
            "explicit direct edge unaffected by derivation",
            g2.tier("x1", "x4") == "direct" and g2.strength("x1", "x4") == 10,
            f"tier={g2.tier('x1', 'x4')} strength={g2.strength('x1', 'x4')}",
        )

    # ---- 15. Phase D: structured output shape ----------------------------
    header("15. Structured output shape (v2) + legacy shape both work")
    g = build_graph(PEOPLE, EDGES)
    r = find_warm_intro(g, ["Alice", "Bob"], "Zoe", top_k=2)
    with tempfile.TemporaryDirectory() as td:
        v2_path = os.path.join(td, "v2.json")
        legacy_path = os.path.join(td, "legacy.json")
        write_result_json_v2(
            v2_path, g, r, entries=["Alice", "Bob"], target="Zoe", top_k=2
        )
        write_result_json(legacy_path, g, r)

        v2 = json.load(open(v2_path))
        legacy = json.load(open(legacy_path))

        # v2 shape: top-level keys
        v2_keys = set(v2.keys())
        expected_v2 = {
            "schema_version", "query", "summary", "nodes", "edges",
            "identity_clusters", "merges", "paths", "explanation",
        }
        check(
            "v2 has expected top-level keys",
            expected_v2 <= v2_keys,
            f"missing: {expected_v2 - v2_keys}",
        )
        check(
            "v2 schema_version is 2",
            v2.get("schema_version") == 2,
            f"got {v2.get('schema_version')}",
        )
        check(
            "v2 summary reports reachability",
            v2["summary"]["reachable"] is True,
            "expected reachable=True",
        )
        check(
            "v2 nodes include all people in the graph",
            len([n for n in v2["nodes"] if n["type"] == "person"])
            == len(g.id_to_name),
            "person node count mismatch",
        )
        check(
            "v2 paths are ranked, best first",
            len(v2["paths"]) >= 1
            and v2["paths"][0]["is_best"] is True
            and v2["paths"][0]["rank"] == 1,
            "best path missing or unranked",
        )
        first_hop = v2["paths"][0]["hops"][0]
        check(
            "v2 hops carry tier, explanation, confidence",
            {"tier", "explanation", "confidence"} <= set(first_hop.keys()),
            f"hop keys: {sorted(first_hop.keys())}",
        )

        # Legacy shape unchanged
        check(
            "legacy still has best_path/alternatives keys",
            "best_path" in legacy and "alternatives" in legacy,
            f"legacy keys: {sorted(legacy.keys())}",
        )
        check(
            "legacy did NOT gain schema_version",
            "schema_version" not in legacy,
            "legacy shape leaked schema_version",
        )

    # ---- 16. Twitter ingester: mutuals + dedup + auto-detect + routing ---
    header("16. Twitter ingester — mutual edges, cross-seed dedup, auto-detect")
    with tempfile.TemporaryDirectory() as td:
        # Mixed column conventions exercise the auto-detector.
        a_following = os.path.join(td, "a_following.csv")
        a_followers = os.path.join(td, "a_followers.csv")
        b_following = os.path.join(td, "b_following.csv")
        b_followers = os.path.join(td, "b_followers.csv")
        with open(a_following, "w", encoding="utf-8") as f:
            f.write("username,display_name\n@bob,Bob\n@charlie,Charlie\n@dave,Dave\n")
        with open(a_followers, "w", encoding="utf-8") as f:
            f.write("username,display_name\n@bob,Bob\n@charlie,Charlie\n@eve,Eve\n")
        with open(b_following, "w", encoding="utf-8") as f:
            f.write("handle,name\nalice,Alice\ncharlie,Charlie\nfrank,Frank\n")
        with open(b_followers, "w", encoding="utf-8") as f:
            f.write("handle,name\nalice,Alice\ncharlie,Charlie\ngrace,Grace\n")

        out = os.path.join(td, "out")
        result = twitter_ingest(
            seeds=[
                ("@alice", a_following, a_followers),
                ("@bob", b_following, b_followers),
            ],
            out_dir=out,
            include_one_way=False,
            edge_tier="mutual",
        )

        # Read back the generated files
        people_rows = list(csv.DictReader(open(result["people"], encoding="utf-8")))
        edge_rows = list(csv.DictReader(open(result["edges"], encoding="utf-8")))
        identity_rows = list(csv.DictReader(open(result["identities"], encoding="utf-8")))

        # 7 unique handles across both seeds (alice/bob/charlie/dave/eve/frank/grace)
        check(
            "ingester emits 7 unique people across both seeds",
            len(people_rows) == 7,
            f"got {len(people_rows)}: {[r['id'] for r in people_rows]}",
        )

        # Mutual edges: (alice,bob), (alice,charlie), (bob,charlie) = 3 total
        check(
            "ingester emits 3 mutual edges (mutual-only mode)",
            len(edge_rows) == 3,
            f"got {len(edge_rows)}: {[(r['from'], r['to']) for r in edge_rows]}",
        )
        edge_pairs = {(r["from"], r["to"]) for r in edge_rows}
        check(
            "specific mutual pairs present",
            edge_pairs == {
                ("tw_alice", "tw_bob"),
                ("tw_alice", "tw_charlie"),
                ("tw_bob", "tw_charlie"),
            },
            f"edges: {sorted(edge_pairs)}",
        )

        # Dedup: charlie appears under both seeds but only once in people.csv
        ids = [r["id"] for r in people_rows]
        check(
            "cross-seed dedup — tw_charlie appears exactly once",
            ids.count("tw_charlie") == 1,
            f"tw_charlie count: {ids.count('tw_charlie')}",
        )

        # Auto-detect picked up display names from BOTH conventions
        names = {r["id"]: r["name"] for r in people_rows}
        check(
            "auto-detect captured names from username/display_name",
            names.get("tw_bob") == "Bob",
            f"tw_bob name: {names.get('tw_bob')!r}",
        )
        check(
            "auto-detect captured names from handle/name",
            names.get("tw_frank") == "Frank",
            f"tw_frank name: {names.get('tw_frank')!r}",
        )

        # Identities map every person to their @handle
        check(
            "identities.csv has one row per person, twitter platform",
            len(identity_rows) == 7
            and all(r["platform"] == "twitter" for r in identity_rows),
            f"got {len(identity_rows)} rows",
        )
        check(
            "identity handles include the @ prefix",
            all(r["handle"].startswith("@") for r in identity_rows),
            f"handles: {[r['handle'] for r in identity_rows]}",
        )

        # End-to-end: feed the output back into the engine and route a query.
        g = build_graph(
            result["people"],
            result["edges"],
            identities_path=result["identities"],
        )
        r = find_warm_intro(g, ["tw_alice"], "tw_charlie", top_k=1)
        check(
            "engine routes tw_alice -> tw_charlie in 1 hop (direct mutual)",
            r["hops"] == 1,
            f"expected 1 hop, got {r['hops']}",
        )
        check(
            "routed edge carries the mutual tier (strength 8)",
            r["total_strength"] == 8,
            f"expected total_strength=8, got {r['total_strength']}",
        )
        check(
            "engine sees twitter accounts on the merged person",
            any(c.person_id == "tw_alice" and "twitter:@alice" in c.account_ids
                for c in g.identity_clusters),
            "tw_alice cluster missing twitter:@alice",
        )

        # --include-one-way should add more edges than mutual-only.
        out2 = os.path.join(td, "out_oneway")
        result2 = twitter_ingest(
            seeds=[
                ("@alice", a_following, a_followers),
                ("@bob", b_following, b_followers),
            ],
            out_dir=out2,
            include_one_way=True,
            edge_tier="mutual",
        )
        edge_rows_2 = list(csv.DictReader(open(result2["edges"], encoding="utf-8")))
        check(
            "--include-one-way produces strictly more edges",
            len(edge_rows_2) > len(edge_rows),
            f"mutual-only={len(edge_rows)}, with one-way={len(edge_rows_2)}",
        )
        # Mutual-tier edges from the first run must still be present at strength 8
        oneway_lookup = {
            (r["from"], r["to"]): (int(r["strength"]), r["tier"])
            for r in edge_rows_2
        }
        check(
            "mutual evidence wins over one-way on dedup",
            oneway_lookup.get(("tw_alice", "tw_bob")) == (8, "mutual"),
            f"got {oneway_lookup.get(('tw_alice', 'tw_bob'))}",
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
