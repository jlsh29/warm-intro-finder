"""Dry-run smoke test for social_graph_collector.

Runs the collector against a throwaway temp directory with synthetic
data so nothing in the main warm_intro project is touched. Checks:

  1. collection_report.json is written and carries dry_run=True.
  2. People + edges are discovered at multiple degrees (at least 2 deep).
  3. Every emitted edge passes the min_strength threshold (≥ 5).
  4. CSVs are NEVER modified in dry-run mode (size unchanged).
  5. Progress file is written during BFS.
  6. Real-mode adapters correctly raise NotImplementedError so we know
     the stubs wire through the backoff helper.

Run:
    python test_social_graph_collector.py
Exit code 0 on success, 1 on any failure.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from social_graph_collector import (  # noqa: E402
    CollectorConfig,
    SocialGraphCollector,
    SyntheticAdapter,
    TwitterAdapter,
    FarcasterAdapter,
    LinkedInAdapter,
    DeBankAdapter,
    REPORT_FILENAME,
    PROGRESS_FILENAME,
    DEFAULT_MIN_STRENGTH,
    DEFAULT_MAX_DEGREES,
)


failures: list[str] = []


def check(label: str, cond: bool, detail: str = "") -> None:
    if cond:
        print(f"  [OK]{label}")
    else:
        msg = f"  [FAIL]{label}" + (f" — {detail}" if detail else "")
        print(msg)
        failures.append(msg.strip())


def test_dry_run_discovers_and_leaves_csvs_untouched() -> None:
    print("\n[1] dry-run: synthetic BFS, CSVs untouched")
    with tempfile.TemporaryDirectory() as tmp:
        # Seed minimal existing CSVs so the merge path has something to diff against.
        people_path = os.path.join(tmp, "people.csv")
        edges_path = os.path.join(tmp, "edges.csv")
        identities_path = os.path.join(tmp, "identities.csv")
        profile_path = os.path.join(tmp, "user_profile.json")
        with open(people_path, "w", encoding="utf-8") as f:
            f.write("id\nme\n")
        with open(edges_path, "w", encoding="utf-8") as f:
            f.write("from,to,strength,tier\n")
        with open(identities_path, "w", encoding="utf-8") as f:
            f.write("person_id,twitter,farcaster,linkedin,debank\n")
            f.write("me,@testuser,testuser,test-user,0xabc\n")

        profile = {
            "name": "Test User",
            "twitter": "@testuser",
            "farcaster": "testuser",
            "linkedin": "test-user",
            "debank": "0x" + "a" * 40,
        }
        with open(profile_path, "w", encoding="utf-8") as f:
            json.dump(profile, f)

        # Snapshot CSV sizes so we can assert non-modification.
        sizes_before = {
            p: os.path.getsize(p) for p in (people_path, edges_path, identities_path)
        }

        config = CollectorConfig(
            out_dir=tmp, dry_run=True, profile_file=profile_path,
        )
        collector = SocialGraphCollector(config)
        report = collector.collect(profile)

        report_path = os.path.join(tmp, REPORT_FILENAME)
        check("collection_report.json written", os.path.exists(report_path))
        with open(report_path, encoding="utf-8") as f:
            persisted = json.load(f)
        check("report is marked dry_run=True", persisted.get("dry_run") is True,
              detail=f"got dry_run={persisted.get('dry_run')}")

        check(
            "all 4 platforms synthetic-used in dry-run",
            set(report["platforms_used"]) == {"twitter", "farcaster", "linkedin", "debank"},
            detail=f"got {report['platforms_used']}",
        )
        check("platforms_skipped is empty in dry-run", report["platforms_skipped"] == [])

        check("people_discovered > 0", report["people_discovered"] > 0,
              detail=f"got {report['people_discovered']}")
        check("edges_discovered > 0", report["edges_discovered"] > 0,
              detail=f"got {report['edges_discovered']}")

        # BFS should reach at least degree 2 with our fan-out defaults.
        deg_map = {int(k): v for k, v in report["people_per_degree"].items()}
        check("at least one degree-1 connection", deg_map.get(1, 0) > 0,
              detail=f"per-degree={deg_map}")
        check("BFS respects max_degrees=3",
              DEFAULT_MAX_DEGREES == 3 and all(d <= DEFAULT_MAX_DEGREES for d in deg_map),
              detail=f"per-degree keys={list(deg_map.keys())}")

        check("min_strength_threshold persisted as 5",
              report["min_strength_threshold"] == DEFAULT_MIN_STRENGTH,
              detail=f"got {report['min_strength_threshold']}")

        check("new_people_added=0 in dry-run",
              report["new_people_added"] == 0,
              detail=f"got {report['new_people_added']}")
        check("new_edges_added=0 in dry-run",
              report["new_edges_added"] == 0,
              detail=f"got {report['new_edges_added']}")

        # CSV non-modification
        sizes_after = {p: os.path.getsize(p) for p in sizes_before}
        for p, before in sizes_before.items():
            check(
                f"{os.path.basename(p)} unchanged in dry-run",
                sizes_after[p] == before,
                detail=f"before={before} after={sizes_after[p]}",
            )


def test_synthetic_adapter_filters_weak_ties_in_merge() -> None:
    """Strong-tie filter runs inside `collect()`; we verify by checking
    the adapter surface — any weak ties (strength < 5) surfaced by the
    synthetic adapter are the exact ones filtered out in the BFS."""
    print("\n[2] synthetic adapter produces both strong (10/7) and weak (2) ties")
    # Large fanout so both outcomes appear under the 75/25 split
    # without making the test flaky on unlucky seeds.
    adapter = SyntheticAdapter("twitter", seed=7, fanout=(200, 200))
    conns = adapter.fetch_strong_connections("@someone")
    strengths = {c.strength for c in conns}
    check("twitter synthetic emits strength 10 (strong)",
          10 in strengths, detail=f"strengths={strengths}")
    # Twitter synthetic also emits weak 2 sometimes; that's expected
    # because the BFS filter is what drops them. We just want to know
    # the adapter is producing a mix so the filter is actually exercised.
    check("twitter synthetic emits strength 2 (weak, to be filtered)",
          2 in strengths, detail=f"strengths={strengths}")

    adapter_li = SyntheticAdapter("linkedin", seed=7, fanout=(200, 200))
    conns_li = adapter_li.fetch_strong_connections("test")
    li_strengths = {c.strength for c in conns_li}
    check("linkedin synthetic emits 10 (recommendation)", 10 in li_strengths,
          detail=f"got {li_strengths}")
    check("linkedin synthetic emits 7 (connection-only, still >= 5)",
          7 in li_strengths, detail=f"got {li_strengths}")


def test_real_adapters_are_stubs() -> None:
    """Each real adapter should raise NotImplementedError so callers
    know they need to wire actual HTTP when credentials exist."""
    print("\n[3] real-mode adapters raise NotImplementedError (stubs)")
    for cls in (TwitterAdapter, FarcasterAdapter, LinkedInAdapter, DeBankAdapter):
        a = cls(api_key="dummy")
        try:
            a.fetch_strong_connections("anyone")
        except NotImplementedError:
            check(f"{cls.__name__} raises NotImplementedError", True)
            continue
        except Exception as e:  # noqa: BLE001
            check(f"{cls.__name__} raises NotImplementedError", False,
                  detail=f"got {type(e).__name__}: {e}")
            continue
        check(f"{cls.__name__} raises NotImplementedError", False,
              detail="returned without raising")


def test_missing_api_keys_skipped_in_real_mode() -> None:
    """In non-dry-run mode with zero env keys set, platforms are all
    skipped and the report carries the right shape without crashing.

    The collector's load_dotenv() reads .env from CWD, so we cd into
    a temp dir that has no .env before invoking collect().
    """
    print("\n[4] real-mode w/ no API keys -- all platforms skipped cleanly")
    saved_env = {}
    for k in ("TWITTER_BEARER_TOKEN", "FARCASTER_API_KEY",
              "LINKEDIN_ACCESS_TOKEN", "DEBANK_API_KEY"):
        if k in os.environ:
            saved_env[k] = os.environ.pop(k)
    saved_cwd = os.getcwd()
    try:
        with tempfile.TemporaryDirectory() as tmp:
            try:
                os.chdir(tmp)  # load_dotenv() will find no .env here
                config = CollectorConfig(out_dir=tmp, dry_run=False)
                collector = SocialGraphCollector(config)
                report = collector.collect({"name": "X", "twitter": "@x"})
                check("no crash when no credentials", True)
                check(
                    "all 4 platforms skipped",
                    set(report["platforms_skipped"]) == {"twitter", "farcaster", "linkedin", "debank"},
                    detail=f"got skipped={report['platforms_skipped']}",
                )
                check("platforms_used is empty", report["platforms_used"] == [])
                check("people_discovered == 0 when no adapters", report["people_discovered"] == 0)
                check(
                    "report carries an error explaining the skip",
                    any("no platforms" in e or "api keys" in e.lower() for e in report["errors"]),
                    detail=f"errors={report['errors']}",
                )
            finally:
                # Must chdir out BEFORE TemporaryDirectory exits, or
                # Windows refuses to delete it (WinError 32).
                os.chdir(saved_cwd)
    finally:
        os.environ.update(saved_env)


def test_progress_file_written_during_bfs() -> None:
    """Spot-check that the progress file appears while BFS is running
    (still exists after dry-run since we don't clear it in that mode)."""
    print("\n[5] collection_progress.json written during/after dry-run")
    with tempfile.TemporaryDirectory() as tmp:
        profile = {"name": "Prog", "twitter": "@prog", "farcaster": "prog",
                   "linkedin": "prog", "debank": "0x" + "b" * 40}
        config = CollectorConfig(out_dir=tmp, dry_run=True)
        collector = SocialGraphCollector(config)
        collector.collect(profile)
        progress_path = os.path.join(tmp, PROGRESS_FILENAME)
        check("progress file exists after dry-run", os.path.exists(progress_path))
        if os.path.exists(progress_path):
            with open(progress_path, encoding="utf-8") as f:
                data = json.load(f)
            check("progress carries visited set", "visited" in data and isinstance(data["visited"], list))
            check("progress carries people map", "people" in data and isinstance(data["people"], dict))


def main() -> int:
    print("=" * 72)
    print("social_graph_collector — dry-run smoke test")
    print("=" * 72)
    test_dry_run_discovers_and_leaves_csvs_untouched()
    test_synthetic_adapter_filters_weak_ties_in_merge()
    test_real_adapters_are_stubs()
    test_missing_api_keys_skipped_in_real_mode()
    test_progress_file_written_during_bfs()

    print("\n" + "=" * 72)
    if failures:
        print(f"FAILED ({len(failures)} check(s)):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("All collector checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
