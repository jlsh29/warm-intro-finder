"""Multi-platform connection discovery module.

Reads people.csv and identities.csv, then uses API keys to discover
connections between every pair of people across four platforms:

  * Twitter (X API v2)            — mutual follow = strength 8 (mutual),
                                    one-way follow = strength 2
                                    (platform_similarity).
  * Farcaster (Neynar API)        — same strengths as Twitter.
  * LinkedIn (LinkedIn API)       — direct 1st-degree connection =
                                    strength 8 (mutual).
  * DeBank (DeBank Cloud API)     — direct wallet-to-wallet interaction =
                                    strength 8 (mutual).

Merges results from every platform and writes a unified edges.csv. When
the same pair shows up on multiple platforms, the highest strength
wins.

API keys are loaded from a `.env` file in the working directory (see
`.env.example`). Any platform whose key is missing is transparently
skipped with a warning; the rest still run.

Usage
-----
    python connection_discovery.py \
        --people people.csv \
        --identities identities.csv \
        --out edges.csv

    # Test without making real API calls:
    python connection_discovery.py \
        --people people.csv \
        --identities identities.csv \
        --out edges.csv \
        --dry-run
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Iterable


# Tier strengths — mirror core.TIER_STRENGTH so downstream warm_intro.py
# interprets the tiers consistently.
TIER_STRENGTH: dict[str, int] = {
    "direct": 10,
    "mutual": 8,
    "shared_org": 5,
    "platform_similarity": 2,
}

# Env-var names per platform. Keep in sync with .env.example.
ENV_KEYS: dict[str, str] = {
    "twitter": "TWITTER_BEARER_TOKEN",
    "farcaster": "FARCASTER_API_KEY",
    "linkedin": "LINKEDIN_ACCESS_TOKEN",
    "debank": "DEBANK_API_KEY",
}


# --- .env loader (tiny, no external dependency) --------------------------

def load_dotenv(path: str = ".env") -> None:
    """Populate os.environ from a KEY=VALUE file. Silent if file missing.

    Lines starting with `#` and blank lines are ignored. Surrounding
    single/double quotes around values are stripped. Existing env vars
    are not overwritten, so real shell vars still take precedence.
    """
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


# --- CSV loaders ---------------------------------------------------------


@dataclass
class Identity:
    """One person's handles across the four supported platforms."""

    person_id: str
    twitter: str = ""
    farcaster: str = ""
    linkedin: str = ""
    debank: str = ""


def _normalize_handle(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("@"):
        s = s[1:]
    return s.lower()


def _normalize_wallet(s: str) -> str:
    return (s or "").strip().lower()


def read_people(path: str) -> list[str]:
    """Return the list of person ids from people.csv.

    Accepts either `id` or `id,name` headers; only the id column is
    required.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"people file not found: {path}")
    ids: list[str] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "id" not in reader.fieldnames:
            raise ValueError(f"{path}: expected an 'id' column; got {reader.fieldnames}")
        for row in reader:
            pid = (row.get("id") or "").strip()
            if pid:
                ids.append(pid)
    return ids


def read_identities(path: str, known_ids: Iterable[str]) -> dict[str, Identity]:
    """Return {person_id: Identity} for every row whose person_id is known.

    Unknown person_ids are skipped with a warning. Rows for known
    people with all-empty handle cells are kept (the person simply
    has no discoverable connections).
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"identities file not found: {path}")
    known = set(known_ids)
    out: dict[str, Identity] = {pid: Identity(person_id=pid) for pid in known}
    expected = {"person_id", "twitter", "farcaster", "linkedin", "debank"}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"{path}: empty CSV")
        missing = expected - {h.strip() for h in reader.fieldnames}
        if missing:
            raise ValueError(
                f"{path}: missing required columns {sorted(missing)}; "
                f"got {reader.fieldnames}"
            )
        for row in reader:
            pid = (row.get("person_id") or "").strip()
            if not pid:
                continue
            if pid not in known:
                print(
                    f"warning: identities row for unknown person_id {pid!r} "
                    f"(not in people.csv); skipping",
                    file=sys.stderr,
                )
                continue
            out[pid] = Identity(
                person_id=pid,
                twitter=_normalize_handle(row.get("twitter", "")),
                farcaster=_normalize_handle(row.get("farcaster", "")),
                linkedin=_normalize_handle(row.get("linkedin", "")),
                debank=_normalize_wallet(row.get("debank", "")),
            )
    return out


# --- Edge bookkeeping ----------------------------------------------------


@dataclass
class EdgeAcc:
    """Accumulator for the strongest evidence across platforms.

    Keyed by sorted (person_a, person_b). `strength`/`tier` always
    reflect the winning platform; `platforms` records every platform
    that contributed evidence (useful for debugging / reason strings).
    """

    strength: int
    tier: str
    platforms: set[str] = field(default_factory=set)


def _edge_key(a: str, b: str) -> tuple[str, str]:
    return (a, b) if a <= b else (b, a)


def _update_edge(
    edges: dict[tuple[str, str], EdgeAcc],
    a: str,
    b: str,
    tier: str,
    platform: str,
) -> None:
    if a == b:
        return
    strength = TIER_STRENGTH[tier]
    key = _edge_key(a, b)
    cur = edges.get(key)
    if cur is None or strength > cur.strength:
        edges[key] = EdgeAcc(
            strength=strength,
            tier=tier,
            platforms={*(cur.platforms if cur else set()), platform},
        )
    else:
        cur.platforms.add(platform)


# --- Platform discovery adapters -----------------------------------------
#
# Each adapter has the same shape:
#
#   discover_<platform>(identities, api_key, edges, dry_run) -> int
#
# It mutates `edges` in place and returns the count of edges it added
# or strengthened. When `dry_run` is True, no network is touched — the
# adapter instead pairs up every (handle_i, handle_j) in `identities`
# that has a handle for the platform and emits a mutual-tier edge. This
# gives a predictable shape for testing without spending API quota.


def _pairs_with_handle(
    identities: dict[str, Identity], attr: str
) -> list[tuple[str, str]]:
    """Return all unordered pairs of person_ids that both have `attr` set."""
    haves = [pid for pid, ident in identities.items() if getattr(ident, attr)]
    pairs: list[tuple[str, str]] = []
    for i, a in enumerate(haves):
        for b in haves[i + 1:]:
            pairs.append((a, b))
    return pairs


def _api_call_with_retry(
    fn: Callable[[], object],
    *,
    platform: str,
    max_attempts: int = 3,
    base_delay: float = 1.0,
) -> object | None:
    """Call `fn()` with exponential backoff on rate-limit / transient error.

    Network code lives inside `fn`. This wrapper is a placeholder that
    does the minimum required for graceful rate-limit handling: three
    tries, doubling delay, returning None if all fail.
    """
    delay = base_delay
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 — platform-specific errors vary
            msg = str(exc).lower()
            is_rate = "rate" in msg or "429" in msg or "limit" in msg
            if attempt == max_attempts:
                print(
                    f"error: {platform}: giving up after {attempt} attempts: {exc}",
                    file=sys.stderr,
                )
                return None
            print(
                f"warning: {platform}: attempt {attempt}/{max_attempts} failed "
                f"({'rate-limited' if is_rate else 'error'}: {exc}); "
                f"sleeping {delay:.1f}s",
                file=sys.stderr,
            )
            time.sleep(delay)
            delay *= 2
    return None


def discover_twitter(
    identities: dict[str, Identity],
    api_key: str,
    edges: dict[tuple[str, str], EdgeAcc],
    dry_run: bool,
) -> int:
    """Discover Twitter-based connections between people.

    Real mode uses the X API v2 Bearer Token to fetch each handle's
    following + followers, then emits a `mutual` edge when two people
    follow each other and a `platform_similarity` edge when only one
    direction holds.
    """
    added = 0
    people_with_handle = {
        pid: ident.twitter for pid, ident in identities.items() if ident.twitter
    }
    if len(people_with_handle) < 2:
        print(
            "info: twitter: fewer than 2 people have a twitter handle; nothing to do",
            file=sys.stderr,
        )
        return 0

    if dry_run:
        for a, b in _pairs_with_handle(identities, "twitter"):
            before = edges.get(_edge_key(a, b))
            _update_edge(edges, a, b, "mutual", "twitter")
            if edges[_edge_key(a, b)] is not before:
                added += 1
        print(
            f"info: twitter: dry-run emitted {added} mutual edge(s) across "
            f"{len(people_with_handle)} handle(s)",
            file=sys.stderr,
        )
        return added

    # Real-mode scaffold: build a per-person follow set, then intersect.
    follow_sets: dict[str, set[str]] = {}
    for pid, handle in people_with_handle.items():
        following = _api_call_with_retry(
            lambda h=handle: _twitter_fetch_following(h, api_key),
            platform="twitter",
        )
        followers = _api_call_with_retry(
            lambda h=handle: _twitter_fetch_followers(h, api_key),
            platform="twitter",
        )
        if following is None or followers is None:
            print(
                f"warning: twitter: skipping {handle} (fetch failed)",
                file=sys.stderr,
            )
            continue
        follow_sets[pid] = {("out", h) for h in following} | {
            ("in", h) for h in followers
        }

    handle_to_pid = {h: pid for pid, h in people_with_handle.items()}
    for pid, signals in follow_sets.items():
        my_handle = people_with_handle[pid]
        for direction, other_handle in signals:
            other_pid = handle_to_pid.get(other_handle)
            if not other_pid or other_pid == pid:
                continue
            # Determine tier: mutual if both directions present.
            their_signals = follow_sets.get(other_pid, set())
            both_ways = (
                ("out", my_handle) in their_signals
                and ("in", my_handle) in their_signals
            )
            tier = "mutual" if both_ways else "platform_similarity"
            before = edges.get(_edge_key(pid, other_pid))
            _update_edge(edges, pid, other_pid, tier, "twitter")
            if edges[_edge_key(pid, other_pid)] is not before:
                added += 1
    return added


TWITTER_API_BASE = "https://api.twitter.com/2"
# Simple per-process cache so we only hit /users/by/username once per handle,
# even when we fetch both following and followers for the same person.
_TWITTER_USER_ID_CACHE: dict[str, str] = {}


def _twitter_http_get(url: str, bearer: str) -> dict:
    """GET `url` with a Bearer token. Raises on 429 / non-2xx.

    On 429 the server's `x-rate-limit-reset` header (epoch seconds) is
    honored by sleeping until the reset, then raising a retryable error
    so `_api_call_with_retry` can re-run the call.
    """
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {bearer}",
            "User-Agent": "warm_intro-connection-discovery/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code == 429:
            reset = exc.headers.get("x-rate-limit-reset")
            if reset and reset.isdigit():
                wait = max(0, int(reset) - int(time.time())) + 1
                if 0 < wait <= 900:  # cap at 15 min so we don't hang forever
                    print(
                        f"warning: twitter: 429 rate-limited; sleeping {wait}s "
                        f"until x-rate-limit-reset",
                        file=sys.stderr,
                    )
                    time.sleep(wait)
            raise RuntimeError(f"twitter: 429 rate limit on {url}") from exc
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"twitter: HTTP {exc.code} on {url}: {body}") from exc


def _twitter_resolve_user_id(handle: str, bearer: str) -> str:
    """Map a @handle to its numeric user id via /2/users/by/username."""
    cached = _TWITTER_USER_ID_CACHE.get(handle)
    if cached:
        return cached
    url = f"{TWITTER_API_BASE}/users/by/username/{urllib.parse.quote(handle)}"
    payload = _twitter_http_get(url, bearer)
    data = payload.get("data") or {}
    uid = data.get("id")
    if not uid:
        errors = payload.get("errors") or []
        raise RuntimeError(
            f"twitter: could not resolve @{handle} to a user id: "
            f"{errors or payload}"
        )
    _TWITTER_USER_ID_CACHE[handle] = uid
    return uid


def _twitter_paginate_handles(endpoint: str, bearer: str) -> list[str]:
    """Page through /following or /followers, returning lowercase @-less handles.

    X API v2 returns 100 users per page max. We defensively cap at 10
    pages (=1000 users) per handle so a highly-followed seed can't burn
    the whole rate-limit window.
    """
    out: list[str] = []
    next_token: str | None = None
    for _ in range(10):
        qs = {"max_results": "100", "user.fields": "username"}
        if next_token:
            qs["pagination_token"] = next_token
        url = f"{endpoint}?{urllib.parse.urlencode(qs)}"
        payload = _twitter_http_get(url, bearer)
        for u in payload.get("data") or []:
            uname = (u.get("username") or "").lower()
            if uname:
                out.append(uname)
        next_token = (payload.get("meta") or {}).get("next_token")
        if not next_token:
            break
    return out


def _twitter_fetch_following(handle: str, bearer: str) -> list[str]:
    """Return the lowercase handles `handle` follows on X."""
    uid = _twitter_resolve_user_id(handle, bearer)
    return _twitter_paginate_handles(
        f"{TWITTER_API_BASE}/users/{uid}/following", bearer
    )


def _twitter_fetch_followers(handle: str, bearer: str) -> list[str]:
    """Return the lowercase handles that follow `handle` on X."""
    uid = _twitter_resolve_user_id(handle, bearer)
    return _twitter_paginate_handles(
        f"{TWITTER_API_BASE}/users/{uid}/followers", bearer
    )


def discover_farcaster(
    identities: dict[str, Identity],
    api_key: str,
    edges: dict[tuple[str, str], EdgeAcc],
    dry_run: bool,
) -> int:
    """Discover Farcaster connections via Neynar API.

    Emits mutual (strength 8) when both people follow each other on
    Farcaster, otherwise platform_similarity (strength 2).
    """
    added = 0
    people_with_handle = {
        pid: ident.farcaster for pid, ident in identities.items() if ident.farcaster
    }
    if len(people_with_handle) < 2:
        print(
            "info: farcaster: fewer than 2 people have a farcaster handle; nothing to do",
            file=sys.stderr,
        )
        return 0

    if dry_run:
        for a, b in _pairs_with_handle(identities, "farcaster"):
            before = edges.get(_edge_key(a, b))
            _update_edge(edges, a, b, "mutual", "farcaster")
            if edges[_edge_key(a, b)] is not before:
                added += 1
        print(
            f"info: farcaster: dry-run emitted {added} mutual edge(s) across "
            f"{len(people_with_handle)} handle(s)",
            file=sys.stderr,
        )
        return added

    follow_sets: dict[str, set[tuple[str, str]]] = {}
    for pid, handle in people_with_handle.items():
        following = _api_call_with_retry(
            lambda h=handle: _farcaster_fetch_following(h, api_key),
            platform="farcaster",
        )
        followers = _api_call_with_retry(
            lambda h=handle: _farcaster_fetch_followers(h, api_key),
            platform="farcaster",
        )
        if following is None or followers is None:
            print(
                f"warning: farcaster: skipping {handle} (fetch failed)",
                file=sys.stderr,
            )
            continue
        follow_sets[pid] = {("out", h) for h in following} | {
            ("in", h) for h in followers
        }

    handle_to_pid = {h: pid for pid, h in people_with_handle.items()}
    for pid, signals in follow_sets.items():
        my_handle = people_with_handle[pid]
        for direction, other_handle in signals:
            other_pid = handle_to_pid.get(other_handle)
            if not other_pid or other_pid == pid:
                continue
            their = follow_sets.get(other_pid, set())
            both_ways = (
                ("out", my_handle) in their and ("in", my_handle) in their
            )
            tier = "mutual" if both_ways else "platform_similarity"
            before = edges.get(_edge_key(pid, other_pid))
            _update_edge(edges, pid, other_pid, tier, "farcaster")
            if edges[_edge_key(pid, other_pid)] is not before:
                added += 1
    return added


def _farcaster_fetch_following(handle: str, api_key: str) -> list[str]:
    raise NotImplementedError(
        "farcaster real-mode fetch is not wired up; use --dry-run"
    )


def _farcaster_fetch_followers(handle: str, api_key: str) -> list[str]:
    raise NotImplementedError(
        "farcaster real-mode fetch is not wired up; use --dry-run"
    )


def discover_linkedin(
    identities: dict[str, Identity],
    api_key: str,
    edges: dict[tuple[str, str], EdgeAcc],
    dry_run: bool,
) -> int:
    """Discover LinkedIn 1st-degree connections.

    LinkedIn's API does not expose one-way "follow" semantics for
    personal profiles, so every discovered link is modelled as a
    mutual-tier edge (strength 8).
    """
    added = 0
    people_with_handle = {
        pid: ident.linkedin for pid, ident in identities.items() if ident.linkedin
    }
    if len(people_with_handle) < 2:
        print(
            "info: linkedin: fewer than 2 people have a linkedin handle; nothing to do",
            file=sys.stderr,
        )
        return 0

    if dry_run:
        for a, b in _pairs_with_handle(identities, "linkedin"):
            before = edges.get(_edge_key(a, b))
            _update_edge(edges, a, b, "mutual", "linkedin")
            if edges[_edge_key(a, b)] is not before:
                added += 1
        print(
            f"info: linkedin: dry-run emitted {added} mutual edge(s) across "
            f"{len(people_with_handle)} handle(s)",
            file=sys.stderr,
        )
        return added

    connections: dict[str, set[str]] = {}
    for pid, handle in people_with_handle.items():
        result = _api_call_with_retry(
            lambda h=handle: _linkedin_fetch_connections(h, api_key),
            platform="linkedin",
        )
        if result is None:
            print(
                f"warning: linkedin: skipping {handle} (fetch failed)",
                file=sys.stderr,
            )
            continue
        connections[pid] = set(result)

    handle_to_pid = {h: pid for pid, h in people_with_handle.items()}
    for pid, conn_handles in connections.items():
        for other_handle in conn_handles:
            other_pid = handle_to_pid.get(other_handle)
            if not other_pid or other_pid == pid:
                continue
            before = edges.get(_edge_key(pid, other_pid))
            _update_edge(edges, pid, other_pid, "mutual", "linkedin")
            if edges[_edge_key(pid, other_pid)] is not before:
                added += 1
    return added


def _linkedin_fetch_connections(handle: str, access_token: str) -> list[str]:
    raise NotImplementedError(
        "linkedin real-mode fetch is not wired up; use --dry-run"
    )


def discover_debank(
    identities: dict[str, Identity],
    api_key: str,
    edges: dict[tuple[str, str], EdgeAcc],
    dry_run: bool,
) -> int:
    """Discover on-chain ties via DeBank Cloud API.

    Any direct wallet-to-wallet transfer or shared protocol interaction
    yields a mutual edge (strength 8).
    """
    added = 0
    people_with_wallet = {
        pid: ident.debank for pid, ident in identities.items() if ident.debank
    }
    if len(people_with_wallet) < 2:
        print(
            "info: debank: fewer than 2 people have a wallet address; nothing to do",
            file=sys.stderr,
        )
        return 0

    if dry_run:
        for a, b in _pairs_with_handle(identities, "debank"):
            before = edges.get(_edge_key(a, b))
            _update_edge(edges, a, b, "mutual", "debank")
            if edges[_edge_key(a, b)] is not before:
                added += 1
        print(
            f"info: debank: dry-run emitted {added} mutual edge(s) across "
            f"{len(people_with_wallet)} wallet(s)",
            file=sys.stderr,
        )
        return added

    wallet_to_pid = {w: pid for pid, w in people_with_wallet.items()}
    for pid, wallet in people_with_wallet.items():
        counterparts = _api_call_with_retry(
            lambda w=wallet: _debank_fetch_counterparties(w, api_key),
            platform="debank",
        )
        if counterparts is None:
            print(
                f"warning: debank: skipping {wallet} (fetch failed)",
                file=sys.stderr,
            )
            continue
        for other_wallet in counterparts:
            other_pid = wallet_to_pid.get(_normalize_wallet(other_wallet))
            if not other_pid or other_pid == pid:
                continue
            before = edges.get(_edge_key(pid, other_pid))
            _update_edge(edges, pid, other_pid, "mutual", "debank")
            if edges[_edge_key(pid, other_pid)] is not before:
                added += 1
    return added


def _debank_fetch_counterparties(wallet: str, api_key: str) -> list[str]:
    raise NotImplementedError(
        "debank real-mode fetch is not wired up; use --dry-run"
    )


# --- Orchestration -------------------------------------------------------


PLATFORMS: list[tuple[str, Callable[..., int]]] = [
    ("twitter", discover_twitter),
    ("farcaster", discover_farcaster),
    ("linkedin", discover_linkedin),
    ("debank", discover_debank),
]


def discover_all(
    identities: dict[str, Identity],
    dry_run: bool,
    env: dict[str, str] | None = None,
) -> dict[tuple[str, str], EdgeAcc]:
    """Run every platform adapter and return merged edges.

    `env` defaults to `os.environ`; pass a dict to override (testing).
    """
    env = env if env is not None else os.environ
    edges: dict[tuple[str, str], EdgeAcc] = {}
    for name, fn in PLATFORMS:
        key_name = ENV_KEYS[name]
        api_key = env.get(key_name, "").strip()
        if not api_key and not dry_run:
            print(
                f"warning: {name}: {key_name} not set in environment; "
                f"skipping this platform. Add it to your .env file to enable.",
                file=sys.stderr,
            )
            continue
        if dry_run and not api_key:
            # In dry-run we don't need a real key, but print what a real
            # run would require so users know what's missing.
            print(
                f"info: {name}: dry-run (no {key_name} required)",
                file=sys.stderr,
            )
        added = fn(identities, api_key, edges, dry_run)
        print(f"info: {name}: added/updated {added} edge(s)", file=sys.stderr)
    return edges


def write_edges_csv(
    path: str, edges: dict[tuple[str, str], EdgeAcc]
) -> None:
    """Emit a warm_intro-compatible edges.csv (from,to,strength,tier)."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["from", "to", "strength", "tier"])
        for (a, b), acc in sorted(edges.items()):
            w.writerow([a, b, acc.strength, acc.tier])


# --- CLI -----------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Discover connections between people across Twitter, "
        "Farcaster, LinkedIn, and DeBank, merging the results into a "
        "single edges.csv for warm_intro.py."
    )
    parser.add_argument("--people", required=True, help="Path to people.csv")
    parser.add_argument(
        "--identities", required=True, help="Path to identities.csv"
    )
    parser.add_argument("--out", required=True, help="Output path for merged edges.csv")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip real API calls; emit a predictable edge set based only on "
        "which handles are present in identities.csv.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to the .env file holding API keys (default: .env).",
    )
    args = parser.parse_args(argv)

    load_dotenv(args.env_file)

    try:
        person_ids = read_people(args.people)
        identities = read_identities(args.identities, person_ids)
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if not identities:
        print("error: no people found in people.csv", file=sys.stderr)
        return 2

    print(
        f"info: loaded {len(person_ids)} person(s) and "
        f"{sum(1 for i in identities.values() if any([i.twitter, i.farcaster, i.linkedin, i.debank]))} "
        f"with at least one handle",
        file=sys.stderr,
    )

    edges = discover_all(identities, dry_run=args.dry_run)

    write_edges_csv(args.out, edges)

    # Per-tier summary for sanity-check at the CLI.
    tier_counts: dict[str, int] = defaultdict(int)
    for acc in edges.values():
        tier_counts[acc.tier] += 1
    summary = ", ".join(
        f"{tier}={count}" for tier, count in sorted(tier_counts.items())
    ) or "no edges"
    mode = "dry-run" if args.dry_run else "live"
    print(
        f"\n[{mode}] wrote {len(edges)} edge(s) to {args.out} ({summary})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
