"""Dataset validator and cleaner.

Two passes:

1. **Structural** — always-safe CSV-integrity fixes: duplicate ids,
   dangling edges, self-loops, non-numeric strengths, orphan identities.

2. **Liveness** — HTTP checks against platform URLs. Only meaningful for
   REAL handles. Synthetic pseudonyms (e.g. SNAP's `@user_<digits>`) are
   skipped with a clear reason — validating them would mass-delete the
   whole dataset, which is never what you want.

Usage
-----
    # dry-run structural + liveness
    python validate_dataset.py --check-liveness

    # actually mutate the CSVs
    python validate_dataset.py --check-liveness --apply

    # structural only (no network)
    python validate_dataset.py

Exit codes: 0 = clean, 1 = issues in dry-run, 2 = issues + applied.
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable


PROFILE_ID = "me"
PSEUDONYM_RE = re.compile(r"^@?user_\d+$", re.IGNORECASE)
EVM_ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)


def _link_for(platform: str, handle: str) -> str | None:
    """Mirror of app.link_for — duplicated here so the validator has
    zero Flask import chain."""
    h = (handle or "").strip()
    if not h:
        return None
    p = (platform or "").strip().lower()
    if p == "twitter":
        return f"https://twitter.com/{h.lstrip('@')}"
    if p == "linkedin":
        if h.startswith("http"):
            return h
        return f"https://linkedin.com/in/{h.lstrip('@')}"
    if p == "farcaster":
        return f"https://warpcast.com/{h.lstrip('@')}"
    if p in ("wallet", "debank"):
        return f"https://debank.com/profile/{h}"
    return None


@dataclass
class StructReport:
    people_total: int = 0
    people_duplicates: int = 0
    people_malformed: int = 0
    edges_total: int = 0
    edges_self_loops: int = 0
    edges_dangling: int = 0
    edges_bad_strength: int = 0
    edges_dupes_merged: int = 0
    ident_total: int = 0
    ident_orphans: int = 0
    ident_empty: int = 0
    isolates: list[str] = field(default_factory=list)

    @property
    def total_issues(self) -> int:
        return (self.people_duplicates + self.people_malformed
                + self.edges_self_loops + self.edges_dangling
                + self.edges_bad_strength + self.edges_dupes_merged
                + self.ident_orphans + self.ident_empty)


@dataclass
class LiveReport:
    skipped_pseudonym: int = 0
    skipped_self: int = 0
    skipped_empty: int = 0
    twitter_checked: int = 0
    twitter_live: int = 0
    twitter_dead: int = 0
    twitter_blocked: int = 0
    farcaster_checked: int = 0
    farcaster_live: int = 0
    farcaster_dead: int = 0
    farcaster_blocked: int = 0
    linkedin_skipped: int = 0
    wallet_checked: int = 0
    wallet_invalid: int = 0
    dead_by_pid: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))


def _read_csv(path: str) -> tuple[list[str], list[list[str]]]:
    if not os.path.exists(path):
        return [], []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        return [], []
    return rows[0], rows[1:]


def _write_csv(path: str, header: list[str], rows: Iterable[list[str]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)


def structural_pass(people_path: str, edges_path: str,
                    identities_path: str) -> tuple[StructReport,
                                                    tuple[list[str], list[list[str]]],
                                                    tuple[list[str], list[list[str]]],
                                                    tuple[list[str], list[list[str]]]]:
    """Return the clean rows for each CSV plus a report. Pure function —
    does not write to disk."""
    rep = StructReport()

    # ---- people.csv ---------------------------------------------------
    p_header, p_rows = _read_csv(people_path)
    rep.people_total = len(p_rows)
    seen_ids: set[str] = set()
    clean_people: list[list[str]] = []
    for row in p_rows:
        if not row or not row[0].strip():
            rep.people_malformed += 1
            continue
        pid = row[0].strip()
        if pid in seen_ids:
            rep.people_duplicates += 1
            continue
        seen_ids.add(pid)
        clean_people.append(row)
    valid_ids = seen_ids

    # ---- edges.csv ----------------------------------------------------
    e_header, e_rows = _read_csv(edges_path)
    rep.edges_total = len(e_rows)
    idx_from = e_header.index("from") if "from" in e_header else 0
    idx_to = e_header.index("to") if "to" in e_header else 1
    idx_strength = e_header.index("strength") if "strength" in e_header else (2 if len(e_header) > 2 else -1)
    merged: dict[frozenset[str], list[str]] = {}
    for row in e_rows:
        if len(row) <= max(idx_from, idx_to):
            continue
        a, b = row[idx_from].strip(), row[idx_to].strip()
        if not a or not b:
            rep.edges_dangling += 1
            continue
        if a == b:
            rep.edges_self_loops += 1
            continue
        if a not in valid_ids or b not in valid_ids:
            rep.edges_dangling += 1
            continue
        try:
            s_val = float(row[idx_strength]) if idx_strength >= 0 and len(row) > idx_strength else 1.0
        except (ValueError, TypeError):
            rep.edges_bad_strength += 1
            continue
        if s_val <= 0:
            rep.edges_bad_strength += 1
            continue
        key = frozenset((a, b))
        if key in merged:
            rep.edges_dupes_merged += 1
            prev = merged[key]
            try:
                prev_s = float(prev[idx_strength]) if idx_strength >= 0 and len(prev) > idx_strength else 1.0
            except (ValueError, TypeError):
                prev_s = 1.0
            if s_val > prev_s:
                merged[key] = row
        else:
            merged[key] = row
    clean_edges = list(merged.values())

    # ---- identities.csv ----------------------------------------------
    i_header, i_rows = _read_csv(identities_path)
    rep.ident_total = len(i_rows)
    # Assume column 0 is person_id.
    platform_cols = [c for c in ("twitter", "farcaster", "linkedin", "debank")
                     if c in i_header]
    clean_idents: list[list[str]] = []
    for row in i_rows:
        if not row:
            continue
        pid = row[0].strip() if row else ""
        if not pid or pid not in valid_ids:
            rep.ident_orphans += 1
            continue
        has_any = any(
            (row[i_header.index(c)].strip() if len(row) > i_header.index(c) else "")
            for c in platform_cols
        )
        if not has_any:
            rep.ident_empty += 1
            continue
        clean_idents.append(row)

    # Isolates: ids that have no edges AND no identity handle after cleanup.
    connected: set[str] = set()
    for row in clean_edges:
        connected.add(row[idx_from].strip())
        connected.add(row[idx_to].strip())
    ident_pids = {r[0].strip() for r in clean_idents}
    for pid in valid_ids:
        if pid == PROFILE_ID:
            continue
        if pid not in connected and pid not in ident_pids:
            rep.isolates.append(pid)

    return rep, (p_header, clean_people), (e_header, clean_edges), (i_header, clean_idents)


def _classify_handle(platform: str, handle: str, pid: str) -> str:
    """Return one of: 'pseudonym', 'self', 'empty', 'check', 'skip-linkedin'."""
    h = (handle or "").strip()
    if not h:
        return "empty"
    if pid == PROFILE_ID:
        return "self"
    if PSEUDONYM_RE.match(h):
        return "pseudonym"
    if platform == "linkedin":
        return "skip-linkedin"
    return "check"


def _http_status(url: str, *, timeout: float) -> tuple[int, str]:
    """HEAD with GET fallback. Returns (status_code, note).
    status_code 0 means connection failed."""
    try:
        req = urllib.request.Request(url, method="HEAD",
                                     headers={"User-Agent": USER_AGENT,
                                              "Accept": "*/*"})
        resp = urllib.request.urlopen(req, timeout=timeout)
        return resp.status, ""
    except urllib.error.HTTPError as e:
        return e.code, ""
    except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
        # Some sites 405 HEAD — retry GET.
        try:
            req = urllib.request.Request(url, method="GET",
                                         headers={"User-Agent": USER_AGENT,
                                                  "Accept": "*/*"})
            resp = urllib.request.urlopen(req, timeout=timeout)
            return resp.status, "via-get"
        except urllib.error.HTTPError as e2:
            return e2.code, "via-get"
        except Exception as e2:
            return 0, f"err:{type(e2).__name__}"


def liveness_pass(ident_header: list[str], ident_rows: list[list[str]],
                  *, sleep: float, timeout: float,
                  limit: int | None) -> LiveReport:
    rep = LiveReport()
    checked_count = 0
    platform_cols = [c for c in ("twitter", "farcaster", "linkedin", "debank")
                     if c in ident_header]
    for row in ident_rows:
        if not row:
            continue
        pid = row[0].strip()
        for col in platform_cols:
            idx = ident_header.index(col)
            if idx >= len(row):
                continue
            handle = row[idx].strip()
            kind = _classify_handle(col, handle, pid)

            if kind == "empty":
                rep.skipped_empty += 1
                continue
            if kind == "self":
                rep.skipped_self += 1
                continue
            if kind == "pseudonym":
                rep.skipped_pseudonym += 1
                continue
            if kind == "skip-linkedin":
                rep.linkedin_skipped += 1
                continue

            # Real handle check.
            if col == "debank":
                # Can't liveness-check; format-validate instead.
                rep.wallet_checked += 1
                if not EVM_ADDR_RE.match(handle):
                    rep.wallet_invalid += 1
                    rep.dead_by_pid[pid].add(col)
                continue

            if limit is not None and checked_count >= limit:
                continue

            url = _link_for(col, handle)
            if not url:
                continue
            checked_count += 1
            code, note = _http_status(url, timeout=timeout)
            live = 200 <= code < 400
            dead = code == 404
            blocked = code in (403, 429) or code == 0

            if col == "twitter":
                rep.twitter_checked += 1
                if dead:
                    rep.twitter_dead += 1
                    rep.dead_by_pid[pid].add(col)
                elif blocked:
                    rep.twitter_blocked += 1
                else:
                    rep.twitter_live += 1 if live else 0
            elif col == "farcaster":
                rep.farcaster_checked += 1
                if dead:
                    rep.farcaster_dead += 1
                    rep.dead_by_pid[pid].add(col)
                elif blocked:
                    rep.farcaster_blocked += 1
                else:
                    rep.farcaster_live += 1 if live else 0

            if sleep > 0:
                time.sleep(sleep)
    return rep


def apply_liveness_mutations(live: LiveReport,
                             people: tuple[list[str], list[list[str]]],
                             edges: tuple[list[str], list[list[str]]],
                             identities: tuple[list[str], list[list[str]]]
                             ) -> tuple[tuple[list[str], list[list[str]]],
                                        tuple[list[str], list[list[str]]],
                                        tuple[list[str], list[list[str]]]]:
    """Given the dead-handle map, return mutated (people, edges, identities)
    with dead handles cleared and fully-empty identity rows cascade-deleted."""
    if not live.dead_by_pid:
        return people, edges, identities

    p_header, p_rows = people
    e_header, e_rows = edges
    i_header, i_rows = identities

    platform_cols = [c for c in ("twitter", "farcaster", "linkedin", "debank")
                     if c in i_header]

    # Mutate identities in place: clear dead platform cells.
    drop_pids: set[str] = set()
    new_idents: list[list[str]] = []
    for row in i_rows:
        pid = row[0].strip() if row else ""
        if pid in live.dead_by_pid:
            for col in live.dead_by_pid[pid]:
                try:
                    idx = i_header.index(col)
                    if idx < len(row):
                        row[idx] = ""
                except ValueError:
                    pass
        # Drop row if empty across all platform cols.
        has_any = any(
            (row[i_header.index(c)].strip() if len(row) > i_header.index(c) else "")
            for c in platform_cols
        )
        if not has_any:
            drop_pids.add(pid)
            continue
        new_idents.append(row)

    if not drop_pids:
        return people, edges, (i_header, new_idents)

    # Cascade: remove those pids from people and edges.
    new_people = [r for r in p_rows if (r and r[0].strip() not in drop_pids)]
    idx_from = e_header.index("from") if "from" in e_header else 0
    idx_to = e_header.index("to") if "to" in e_header else 1
    new_edges = [
        r for r in e_rows
        if len(r) > max(idx_from, idx_to)
        and r[idx_from].strip() not in drop_pids
        and r[idx_to].strip() not in drop_pids
    ]
    return (p_header, new_people), (e_header, new_edges), (i_header, new_idents)


def print_report(rep: StructReport, live: LiveReport | None,
                 applied: bool, dry_run_had_issues: bool) -> None:
    print("== Structural ==")
    print(f"  people.csv     : {rep.people_total} rows, "
          f"{rep.people_duplicates} duplicates, {rep.people_malformed} malformed")
    print(f"  edges.csv      : {rep.edges_total} rows, "
          f"{rep.edges_self_loops} self-loops, {rep.edges_dangling} dangling, "
          f"{rep.edges_bad_strength} bad strength, {rep.edges_dupes_merged} merged")
    print(f"  identities.csv : {rep.ident_total} rows, "
          f"{rep.ident_orphans} orphans, {rep.ident_empty} empty")
    if rep.isolates:
        print(f"  isolates       : {len(rep.isolates)} (no edges AND no identity)")
    if live is not None:
        print()
        print("== Liveness ==")
        print(f"  skipped (pseudonym)    : {live.skipped_pseudonym}")
        print(f"  skipped (me / profile) : {live.skipped_self}")
        print(f"  skipped (empty)        : {live.skipped_empty}")
        print(f"  twitter     checked {live.twitter_checked}, "
              f"live {live.twitter_live}, dead {live.twitter_dead}, "
              f"blocked {live.twitter_blocked}")
        print(f"  farcaster   checked {live.farcaster_checked}, "
              f"live {live.farcaster_live}, dead {live.farcaster_dead}, "
              f"blocked {live.farcaster_blocked}")
        print(f"  linkedin    skipped {live.linkedin_skipped} (unreliable without login)")
        print(f"  wallet      format-checked {live.wallet_checked}, "
              f"invalid {live.wallet_invalid}")
        if live.dead_by_pid:
            print(f"  dead handles found on {len(live.dead_by_pid)} row(s):")
            for pid, cols in list(live.dead_by_pid.items())[:10]:
                print(f"    - {pid}: {', '.join(sorted(cols))}")
            if len(live.dead_by_pid) > 10:
                print(f"    ... and {len(live.dead_by_pid) - 10} more")
    print()
    print("== Result ==")
    if applied:
        print("  WROTE cleaned CSVs to disk.")
    else:
        print("  dry-run: no files modified (use --apply to write changes)")
    print(f"  clean: {'yes' if not dry_run_had_issues else 'no -- issues above'}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--people",     default="people.csv")
    p.add_argument("--edges",      default="edges.csv")
    p.add_argument("--identities", default="identities.csv")
    p.add_argument("--check-structure", dest="check_structure",
                   action="store_true", default=True)
    p.add_argument("--no-check-structure", dest="check_structure",
                   action="store_false")
    p.add_argument("--check-liveness", action="store_true")
    p.add_argument("--apply", action="store_true",
                   help="Write mutated CSVs back to disk (default: dry-run)")
    p.add_argument("--sleep", type=float, default=0.8,
                   help="Seconds between HTTP checks")
    p.add_argument("--timeout", type=float, default=8.0,
                   help="Per-request timeout")
    p.add_argument("--limit", type=int, default=None,
                   help="Cap on liveness checks per run")
    args = p.parse_args()

    rep, people, edges, idents = structural_pass(
        args.people, args.edges, args.identities,
    )

    live: LiveReport | None = None
    if args.check_liveness:
        live = liveness_pass(idents[0], idents[1],
                             sleep=args.sleep, timeout=args.timeout,
                             limit=args.limit)
        if args.apply:
            people, edges, idents = apply_liveness_mutations(
                live, people, edges, idents,
            )

    issues = rep.total_issues + (len(live.dead_by_pid) if live else 0)

    if args.apply:
        _write_csv(args.people, *people)
        _write_csv(args.edges, *edges)
        _write_csv(args.identities, *idents)

    print_report(rep, live, applied=args.apply, dry_run_had_issues=(issues > 0))

    if issues == 0:
        return 0
    return 2 if args.apply else 1


if __name__ == "__main__":
    sys.exit(main())
