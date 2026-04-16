"""Background social-graph collector for the Warm Intro Finder.

Starts from `user_profile.json` and walks outward up to 3 degrees,
collecting only STRONG ties (actual interactions, not just follows).
Merged output lands in the same three CSVs the engine already reads:

    people.csv       — new person ids appended (id column)
    edges.csv        — new person-person edges appended (from,to,strength,tier)
    identities.csv   — new wide-format rows appended (person_id,twitter,farcaster,linkedin,debank)

Plus two new sidecars:

    collection_report.json    — summary of the last run
    collection_progress.json  — BFS queue + visited set; enables resume

Strength rules (keeps min_strength ≥ 5; weaker ties are dropped):

    Twitter     mutual follow + likes/replies  → 10  (direct)
                mutual follow only             → 2   (excluded by filter)
    Farcaster   mutual follow + recasts/replies→ 10  (direct)
                mutual follow only             → 2   (excluded)
    LinkedIn    connected + recommendation     → 10  (direct)
                connected only                 → 7   (mutual; *included*, weak)
    DeBank     on-chain transactions together → 10  (direct)
                following only                 → 2   (excluded)

Auto-trigger on profile save is available via `--watch`: it polls
`user_profile.json` mtime and re-runs the collector whenever the file
changes. Run once in a background terminal to get zero-effort
collection on every profile save:

    python social_graph_collector.py --watch

Or run one-shot after saving the profile:

    python social_graph_collector.py
    python social_graph_collector.py --dry-run   # synthetic, CSVs untouched
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable


# ---------------------------------------------------------------- constants

ENV_KEYS: dict[str, str] = {
    "twitter":   "TWITTER_BEARER_TOKEN",
    "farcaster": "FARCASTER_API_KEY",
    "linkedin":  "LINKEDIN_ACCESS_TOKEN",
    "debank":    "DEBANK_API_KEY",
}

# Tier → strength table per interaction type.
STRONG_EDGE_WEIGHTS: dict[str, dict[str, int]] = {
    "twitter":   {"interaction": 10, "follow_only": 2},
    "farcaster": {"interaction": 10, "follow_only": 2},
    "linkedin":  {"recommendation": 10, "connection": 7},
    "debank":    {"transaction": 10, "follow_only": 2},
}

# Column in identities.csv → platform in this module. DeBank is stored
# under a column literally named "debank" but the engine treats it as
# the "wallet" platform.
CSV_PLATFORM_COLUMN = {
    "twitter": "twitter",
    "farcaster": "farcaster",
    "linkedin": "linkedin",
    "debank": "debank",   # "wallet" in runtime graph
}

ID_PREFIX = {
    "twitter": "tw",
    "farcaster": "fc",
    "linkedin": "li",
    "debank": "wal",
}

DEFAULT_MIN_STRENGTH = 5
DEFAULT_MAX_DEGREES = 3
PROGRESS_FILENAME = "collection_progress.json"
REPORT_FILENAME = "collection_report.json"
PROFILE_SEED_ID = "me"


# ---------------------------------------------------------------- .env + profile IO

def load_dotenv(path: str = ".env") -> None:
    """Minimal .env loader (no external dependency). Never overwrites
    existing env vars, so real shell environment wins."""
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


def load_profile(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


# ---------------------------------------------------------------- progress IO

def load_progress(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_progress(state: dict, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def clear_progress(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)


# ---------------------------------------------------------------- rate-limit wrapper

def with_backoff(
    fn: Callable[[], object],
    *,
    platform: str,
    max_attempts: int = 3,
    base_delay: float = 1.0,
) -> object | None:
    """Retry `fn()` with exponential backoff on transient errors.

    Recognises 429/rate-limit responses and waits longer between retries.
    Returns whatever `fn` returns, or None if every attempt failed.
    """
    delay = base_delay
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except NotImplementedError:
            # Real-mode adapters are stubs; bubble up so caller can report.
            raise
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            is_rate = "429" in msg or "rate" in msg or "limit" in msg
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


# ---------------------------------------------------------------- adapters

# Per-platform interaction-count columns written into identities.csv
# by the collector (mirrors seed_profile_network.INTERACTION_COLUMNS).
INTERACTION_COLUMNS = (
    "twitter_likes", "twitter_comments", "twitter_reposts",
    "farcaster_recasts", "farcaster_replies",
    "linkedin_recommendations", "linkedin_endorsements",
    "debank_transactions",
)

INTERACTION_SCORE_CAP = 15


def _score_for_count(n: int) -> int:
    if n <= 0:
        return 0
    if n <= 5:
        return 1
    if n <= 20:
        return 3
    if n <= 50:
        return 5
    return 7


def compute_interaction_score(counts: dict[str, int]) -> int:
    total = sum(_score_for_count(counts.get(c, 0)) for c in INTERACTION_COLUMNS)
    return min(total, INTERACTION_SCORE_CAP)


@dataclass
class StrongConnection:
    platform: str
    handle: str
    name: str
    strength: int
    tier: str       # "direct" | "mutual" | "platform_similarity"
    interaction: str  # "interaction" | "recommendation" | "connection" | ...
    # Per-platform interaction counts for the *discovered* person.
    # Only the keys relevant to `platform` will be non-zero in general.
    counts: dict = field(default_factory=dict)


class PlatformAdapter:
    """Base class; real adapters override `fetch_strong_connections`."""

    platform = ""

    def __init__(self, api_key: str):
        self.api_key = api_key

    def has_credentials(self) -> bool:
        return bool(self.api_key)

    def fetch_strong_connections(self, handle: str) -> list[StrongConnection]:
        raise NotImplementedError


class TwitterAdapter(PlatformAdapter):
    """X API v2: requires paginating /following+/followers and then
    querying /liking_users or /2/users/:id/liked_tweets to establish
    interaction. Real implementation is out of scope — dev-account
    credits are required and vary by tier."""

    platform = "twitter"

    def fetch_strong_connections(self, handle: str) -> list[StrongConnection]:
        raise NotImplementedError(
            "Twitter real-mode fetch not implemented. Use --dry-run, "
            "or wire up X API v2 calls: /2/users/:id/following + /followers "
            "+ /tweets/:id/liking_users."
        )


class FarcasterAdapter(PlatformAdapter):
    """Neynar API: /farcaster/user/bulk, /farcaster/followers,
    /farcaster/following, /farcaster/reactions. Real implementation
    stubbed."""

    platform = "farcaster"

    def fetch_strong_connections(self, handle: str) -> list[StrongConnection]:
        raise NotImplementedError(
            "Farcaster real-mode fetch not implemented. Use --dry-run, "
            "or wire up Neynar /farcaster/followers + reaction endpoints."
        )


class LinkedInAdapter(PlatformAdapter):
    """LinkedIn API is severely restricted for general use; only
    Marketing Developer Platform partners can list connections. Real
    implementation stubbed."""

    platform = "linkedin"

    def fetch_strong_connections(self, handle: str) -> list[StrongConnection]:
        raise NotImplementedError(
            "LinkedIn real-mode fetch not implemented. Use --dry-run."
        )


class DeBankAdapter(PlatformAdapter):
    """DeBank Cloud API: /v1/user/history_list, /v1/user/all_token_list.
    Real implementation stubbed."""

    platform = "debank"

    def fetch_strong_connections(self, handle: str) -> list[StrongConnection]:
        raise NotImplementedError(
            "DeBank real-mode fetch not implemented. Use --dry-run."
        )


class SyntheticAdapter(PlatformAdapter):
    """Dry-run adapter that fabricates plausible strong connections.

    Deterministic given a seed + handle + platform, so repeated dry runs
    produce identical reports (good for testing). Fan-out is capped so
    the BFS terminates quickly.
    """

    platform = ""
    _FIRST = ["Sam", "Alex", "Jordan", "Taylor", "Morgan", "Casey", "Drew",
              "Sky", "Nico", "Ren", "Evan", "Quinn", "Sage", "River", "Amani"]
    _LAST = ["Rivera", "Chen", "Patel", "Osei", "Nakamura", "Silva", "Becker",
             "Ahmad", "Kowalski", "Bergström", "Morales", "Okafor", "Tanaka"]

    def __init__(self, platform: str, *, seed: int = 42, fanout: tuple[int, int] = (2, 4)):
        super().__init__(api_key="dry-run")
        self.platform = platform
        self._seed = seed
        self._fanout = fanout

    def _rng_for(self, handle: str) -> random.Random:
        # Deterministic per (seed, platform, handle).
        h = abs(hash((self._seed, self.platform, handle.lower())))
        return random.Random(h)

    def fetch_strong_connections(self, handle: str) -> list[StrongConnection]:
        rng = self._rng_for(handle)
        count = rng.randint(*self._fanout)
        out: list[StrongConnection] = []
        for _ in range(count):
            name = f"{rng.choice(self._FIRST)} {rng.choice(self._LAST)}"
            # Model interaction-vs-follow probability per platform spec.
            if self.platform == "linkedin":
                if rng.random() < 0.55:
                    tier, strength, interaction = "direct", 10, "recommendation"
                else:
                    tier, strength, interaction = "mutual", 7, "connection"
            else:
                # Twitter / Farcaster / DeBank: strong = 10, weak = 2 (filtered out).
                if rng.random() < 0.75:
                    tier, strength, interaction = "direct", 10, "interaction"
                else:
                    tier, strength, interaction = "platform_similarity", 2, "follow_only"
            new_handle = self._synth_handle(name, rng)
            # Generate plausible per-platform interaction counts for the
            # discovered person. Only populate the column bucket that
            # maps to this adapter's platform; other platforms stay 0
            # (collected on their own sweep if the target's also on them).
            counts = {c: 0 for c in INTERACTION_COLUMNS}
            if self.platform == "twitter":
                counts["twitter_likes"]    = rng.choices([0, 3, 12, 35, 75], [0.1, 0.3, 0.3, 0.2, 0.1])[0]
                counts["twitter_comments"] = rng.choices([0, 2, 8, 25, 60], [0.15, 0.35, 0.3, 0.15, 0.05])[0]
                counts["twitter_reposts"]  = rng.choices([0, 2, 7, 22], [0.2, 0.4, 0.3, 0.1])[0]
            elif self.platform == "farcaster":
                counts["farcaster_recasts"] = rng.choices([0, 2, 8, 25], [0.2, 0.4, 0.3, 0.1])[0]
                counts["farcaster_replies"] = rng.choices([0, 3, 10, 30, 55], [0.15, 0.35, 0.3, 0.15, 0.05])[0]
            elif self.platform == "linkedin":
                counts["linkedin_recommendations"] = rng.choices([0, 1, 3, 6], [0.55, 0.3, 0.12, 0.03])[0]
                counts["linkedin_endorsements"]    = rng.choices([0, 2, 8, 22, 60], [0.2, 0.35, 0.25, 0.15, 0.05])[0]
            elif self.platform == "debank":
                counts["debank_transactions"] = rng.choices([0, 1, 5, 15, 40, 80], [0.3, 0.25, 0.2, 0.15, 0.08, 0.02])[0]
            out.append(StrongConnection(
                platform=self.platform,
                handle=new_handle,
                name=name,
                strength=strength,
                tier=tier,
                interaction=interaction,
                counts=counts,
            ))
        return out

    def _synth_handle(self, name: str, rng: random.Random) -> str:
        base = name.lower().replace(" ", "")
        suf = "".join(rng.choice("0123456789") for _ in range(rng.randint(0, 2)))
        if self.platform == "debank":
            return "0x" + "".join(rng.choice("0123456789abcdef") for _ in range(40))
        if self.platform == "linkedin":
            return name.lower().replace(" ", "-")
        if self.platform == "twitter":
            return f"@{base}{suf}"
        return f"{base}{suf}"  # farcaster: no @


# ---------------------------------------------------------------- collector

@dataclass
class CollectorConfig:
    out_dir: str = "."
    dry_run: bool = False
    max_degrees: int = DEFAULT_MAX_DEGREES
    min_strength: int = DEFAULT_MIN_STRENGTH
    people_file: str = "people.csv"
    edges_file: str = "edges.csv"
    identities_file: str = "identities.csv"
    profile_file: str = "user_profile.json"
    # Synthetic adapter controls (dry-run only)
    synthetic_seed: int = 42
    synthetic_fanout: tuple[int, int] = (2, 4)


REAL_ADAPTERS: dict[str, type[PlatformAdapter]] = {
    "twitter":   TwitterAdapter,
    "farcaster": FarcasterAdapter,
    "linkedin":  LinkedInAdapter,
    "debank":    DeBankAdapter,
}


@dataclass
class DiscoveredPerson:
    pid: str
    name: str
    accounts: list[tuple[str, str]] = field(default_factory=list)  # [(platform, handle), ...]
    degree: int = 0
    # Aggregated per-platform interaction counts. Accumulates across
    # every connection the BFS discovers involving this person.
    counts: dict = field(default_factory=lambda: {c: 0 for c in INTERACTION_COLUMNS})


@dataclass
class DiscoveredEdge:
    from_id: str
    to_id: str
    strength: int
    tier: str
    platform: str
    interaction: str


class SocialGraphCollector:
    """Runs the BFS, filters weak ties, merges into CSVs, writes report."""

    def __init__(self, config: CollectorConfig):
        self.config = config
        os.makedirs(config.out_dir, exist_ok=True)
        self._report_path = os.path.join(config.out_dir, REPORT_FILENAME)
        self._progress_path = os.path.join(config.out_dir, PROGRESS_FILENAME)

    # ---- adapter selection ------------------------------------------------

    def _build_adapters(self) -> tuple[dict[str, PlatformAdapter], list[str], list[str]]:
        """Returns (adapters_by_platform, used, skipped)."""
        used: list[str] = []
        skipped: list[str] = []
        adapters: dict[str, PlatformAdapter] = {}

        if self.config.dry_run:
            for p in ENV_KEYS:
                adapters[p] = SyntheticAdapter(
                    p, seed=self.config.synthetic_seed,
                    fanout=self.config.synthetic_fanout,
                )
                used.append(p)
            return adapters, used, skipped

        load_dotenv()
        for p, env_key in ENV_KEYS.items():
            key = os.environ.get(env_key, "").strip()
            if not key:
                skipped.append(p)
                continue
            adapters[p] = REAL_ADAPTERS[p](key)
            used.append(p)
        return adapters, used, skipped

    # ---- helpers ----------------------------------------------------------

    @staticmethod
    def _profile_handle(profile: dict, platform: str) -> str:
        raw = (profile.get(CSV_PLATFORM_COLUMN[platform]) or "").strip()
        return raw

    @staticmethod
    def _pid_for(platform: str, handle: str) -> str:
        h = handle.lower().lstrip("@")
        return f"{ID_PREFIX[platform]}_{h}"

    @staticmethod
    def _edge_key(a: str, b: str) -> tuple[str, str]:
        return (a, b) if a <= b else (b, a)

    # ---- main entry -------------------------------------------------------

    def collect(self, profile: dict) -> dict:
        started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        adapters, used, skipped = self._build_adapters()

        if not adapters:
            report = self._empty_report(started_at, used, skipped,
                                        errors=["no platforms available — add API keys to .env"])
            self._write_report(report)
            return report

        if not profile:
            report = self._empty_report(started_at, used, skipped,
                                        errors=[f"profile not found at {self.config.profile_file}"])
            self._write_report(report)
            return report

        # Resume-capable BFS state
        progress = load_progress(self._progress_path)
        if progress and progress.get("profile_name") == profile.get("name"):
            queue: list[dict] = progress.get("queue", [])
            visited: set[str] = set(progress.get("visited", []))
            people: dict[str, DiscoveredPerson] = {
                pid: DiscoveredPerson(**p) for pid, p in progress.get("people", {}).items()
            }
            # Rehydrate tuples from the JSON list representation
            for dp in people.values():
                dp.accounts = [tuple(a) for a in dp.accounts]
            edges: dict[tuple[str, str], DiscoveredEdge] = {
                tuple(k.split("|", 1)): DiscoveredEdge(**e)
                for k, e in progress.get("edges", {}).items()
            }
            resumed = True
        else:
            queue, visited, people, edges, resumed = [], set(), {}, {}, False

        if not resumed:
            me = DiscoveredPerson(pid=PROFILE_SEED_ID, name=profile.get("name") or "You", degree=0)
            for platform in used:
                handle = self._profile_handle(profile, platform)
                if not handle:
                    continue
                me.accounts.append((platform, handle))
                queue.append({"pid": PROFILE_SEED_ID, "platform": platform,
                              "handle": handle, "degree": 0})
            if not me.accounts:
                report = self._empty_report(started_at, used, skipped,
                                            errors=["profile has no handles on any supported platform"])
                self._write_report(report)
                return report
            people[PROFILE_SEED_ID] = me

        errors: list[str] = []
        per_degree_counts: dict[int, int] = {d: 0 for d in range(0, self.config.max_degrees + 1)}
        per_degree_counts[0] = 1

        while queue:
            item = queue.pop(0)
            platform = item["platform"]
            handle = item["handle"]
            degree = item["degree"]
            pid = item["pid"]

            # One visit per (platform, handle) — dedup prevents cycles.
            visit_key = f"{platform}:{handle.lower().lstrip('@')}"
            if visit_key in visited:
                continue
            visited.add(visit_key)

            # At max_degrees we only RECORD discoveries made at max-1 (so
            # edge-endpoints exist); we don't query deeper.
            if degree >= self.config.max_degrees:
                continue

            adapter = adapters.get(platform)
            if not adapter:
                continue

            try:
                connections = with_backoff(
                    lambda: adapter.fetch_strong_connections(handle),
                    platform=platform,
                )
            except NotImplementedError as exc:
                errors.append(f"{platform}: {exc}")
                continue
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{platform}:{handle}: {exc}")
                continue
            if connections is None:
                continue  # backoff gave up

            for c in connections:
                # ---- STRONG-TIE FILTER ----
                if c.strength < self.config.min_strength:
                    continue

                other_pid = self._pid_for(c.platform, c.handle)
                if other_pid == pid:
                    continue  # no self-edges

                dp = people.get(other_pid)
                if dp is None:
                    dp = DiscoveredPerson(
                        pid=other_pid, name=c.name, accounts=[(c.platform, c.handle)],
                        degree=degree + 1,
                    )
                    people[other_pid] = dp
                    new_deg = min(degree + 1, self.config.max_degrees)
                    per_degree_counts[new_deg] = per_degree_counts.get(new_deg, 0) + 1
                else:
                    if (c.platform, c.handle) not in dp.accounts:
                        dp.accounts.append((c.platform, c.handle))
                    if (degree + 1) < dp.degree:
                        dp.degree = degree + 1
                # Merge per-platform interaction counts into the discovered
                # person's running totals. MAX (not sum) avoids inflating
                # counts when the same person is discovered via multiple
                # BFS paths.
                for k, v in (c.counts or {}).items():
                    if k in dp.counts and v > dp.counts[k]:
                        dp.counts[k] = v

                key = self._edge_key(pid, other_pid)
                existing = edges.get(key)
                if existing is None or c.strength > existing.strength:
                    edges[key] = DiscoveredEdge(
                        from_id=key[0], to_id=key[1],
                        strength=c.strength, tier=c.tier,
                        platform=c.platform, interaction=c.interaction,
                    )

                # Enqueue for next level (if within max_degrees).
                if (degree + 1) < self.config.max_degrees:
                    queue.append({
                        "pid": other_pid, "platform": c.platform,
                        "handle": c.handle, "degree": degree + 1,
                    })

            # Persist progress after every batch so a crash can resume.
            self._snapshot_progress(profile, queue, visited, people, edges)

        new_people_count, new_edges_count = 0, 0
        if not self.config.dry_run:
            new_people_count, new_edges_count = self._merge_into_csvs(people, edges)
            clear_progress(self._progress_path)

        report = {
            "started_at": started_at,
            "completed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "dry_run": self.config.dry_run,
            "profile_name": profile.get("name") or "",
            "platforms_used": sorted(used),
            "platforms_skipped": sorted(skipped),
            "min_strength_threshold": self.config.min_strength,
            "max_degrees": self.config.max_degrees,
            "people_discovered": len(people),
            "edges_discovered": len(edges),
            "people_per_degree": {str(k): v for k, v in sorted(per_degree_counts.items())},
            "new_people_added": new_people_count,
            "new_edges_added": new_edges_count,
            "errors": errors,
        }
        self._write_report(report)
        return report

    # ---- progress + report writers ----------------------------------------

    def _snapshot_progress(
        self,
        profile: dict,
        queue: list[dict],
        visited: set[str],
        people: dict[str, DiscoveredPerson],
        edges: dict[tuple[str, str], DiscoveredEdge],
    ) -> None:
        save_progress(
            {
                "profile_name": profile.get("name") or "",
                "queue": queue,
                "visited": sorted(visited),
                "people": {
                    pid: {"pid": dp.pid, "name": dp.name,
                          "accounts": list(dp.accounts), "degree": dp.degree,
                          "counts": dict(dp.counts)}
                    for pid, dp in people.items()
                },
                "edges": {
                    f"{k[0]}|{k[1]}": {
                        "from_id": e.from_id, "to_id": e.to_id,
                        "strength": e.strength, "tier": e.tier,
                        "platform": e.platform, "interaction": e.interaction,
                    }
                    for k, e in edges.items()
                },
            },
            self._progress_path,
        )

    def _write_report(self, report: dict) -> None:
        with open(self._report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

    def _empty_report(self, started_at: str, used: list[str], skipped: list[str],
                      errors: list[str]) -> dict:
        return {
            "started_at": started_at,
            "completed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "dry_run": self.config.dry_run,
            "profile_name": "",
            "platforms_used": sorted(used),
            "platforms_skipped": sorted(skipped),
            "min_strength_threshold": self.config.min_strength,
            "max_degrees": self.config.max_degrees,
            "people_discovered": 0,
            "edges_discovered": 0,
            "people_per_degree": {},
            "new_people_added": 0,
            "new_edges_added": 0,
            "errors": errors,
        }

    # ---- CSV merging ------------------------------------------------------

    def _merge_into_csvs(
        self,
        people: dict[str, DiscoveredPerson],
        edges: dict[tuple[str, str], DiscoveredEdge],
    ) -> tuple[int, int]:
        """Append-only merge. Never rewrites rows the engine already routes."""
        people_path = os.path.join(self.config.out_dir, self.config.people_file)
        edges_path = os.path.join(self.config.out_dir, self.config.edges_file)
        identities_path = os.path.join(self.config.out_dir, self.config.identities_file)

        existing_ids = self._read_existing_people(people_path)
        existing_edges = self._read_existing_edges(edges_path)
        existing_identities = self._read_existing_identities(identities_path)

        # --- people.csv (append new ids; header stays "id") ---
        new_people = 0
        people_needs_header = not os.path.exists(people_path)
        with open(people_path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if people_needs_header:
                w.writerow(["id"])
            for pid in sorted(people):
                if pid == PROFILE_SEED_ID or pid in existing_ids:
                    continue
                w.writerow([pid])
                new_people += 1

        # --- identities.csv (append new person rows, with interaction cols) ---
        identities_header = [
            "person_id", "twitter", "farcaster", "linkedin", "debank",
            *INTERACTION_COLUMNS,
            "last_interaction", "interaction_score",
        ]
        id_needs_header = not os.path.exists(identities_path)
        today = datetime.now(timezone.utc).date().isoformat()
        with open(identities_path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if id_needs_header:
                w.writerow(identities_header)
            for pid in sorted(people):
                if pid == PROFILE_SEED_ID or pid in existing_identities:
                    continue
                dp = people[pid]
                plats = {"twitter": "", "farcaster": "", "linkedin": "", "debank": ""}
                for plat, handle in dp.accounts:
                    if plat in plats and not plats[plat]:
                        plats[plat] = handle
                counts = dp.counts or {c: 0 for c in INTERACTION_COLUMNS}
                score = compute_interaction_score(counts)
                last = today if any(counts.values()) else ""
                w.writerow([
                    pid, plats["twitter"], plats["farcaster"],
                    plats["linkedin"], plats["debank"],
                    *(counts.get(c, 0) for c in INTERACTION_COLUMNS),
                    last, score,
                ])

        # --- edges.csv (append new edges, keep stronger on duplicate) ---
        new_edges = 0
        edges_needs_header = not os.path.exists(edges_path)
        with open(edges_path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if edges_needs_header:
                w.writerow(["from", "to", "strength", "tier"])
            for key, e in sorted(edges.items()):
                # Skip if already in existing edges with >= strength
                prev = existing_edges.get(key)
                if prev is not None and prev >= e.strength:
                    continue
                w.writerow([e.from_id, e.to_id, e.strength, e.tier])
                new_edges += 1

        return new_people, new_edges

    @staticmethod
    def _read_existing_people(path: str) -> set[str]:
        if not os.path.exists(path):
            return set()
        out: set[str] = set()
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                pid = (row.get("id") or "").strip()
                if pid:
                    out.add(pid)
        return out

    @staticmethod
    def _read_existing_edges(path: str) -> dict[tuple[str, str], float]:
        if not os.path.exists(path):
            return {}
        out: dict[tuple[str, str], float] = {}
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                a = (row.get("from") or "").strip()
                b = (row.get("to") or "").strip()
                if not a or not b:
                    continue
                try:
                    s = float(row.get("strength") or 0)
                except ValueError:
                    s = 0.0
                key = (a, b) if a <= b else (b, a)
                prev = out.get(key)
                if prev is None or s > prev:
                    out[key] = s
        return out

    @staticmethod
    def _read_existing_identities(path: str) -> set[str]:
        if not os.path.exists(path):
            return set()
        out: set[str] = set()
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                pid = (row.get("person_id") or "").strip()
                if pid:
                    out.add(pid)
        return out


# ---------------------------------------------------------------- CLI

def _run_once(args: argparse.Namespace) -> int:
    profile = load_profile(args.profile)
    if not profile:
        print(f"error: profile not found at {args.profile}", file=sys.stderr)
        return 2
    config = CollectorConfig(
        out_dir=args.out_dir,
        dry_run=args.dry_run,
        profile_file=args.profile,
    )
    collector = SocialGraphCollector(config)
    report = collector.collect(profile)
    mode = "dry-run" if args.dry_run else "live"
    print(
        f"\n[{mode}] discovered {report['people_discovered']} people and "
        f"{report['edges_discovered']} edges "
        f"(degrees {report['people_per_degree']}); "
        f"added {report['new_people_added']} new people, "
        f"{report['new_edges_added']} new edges. "
        f"Platforms used: {report['platforms_used']}; "
        f"skipped: {report['platforms_skipped']}."
    )
    if report["errors"]:
        print(f"\n{len(report['errors'])} error(s):", file=sys.stderr)
        for e in report["errors"][:5]:
            print(f"  - {e}", file=sys.stderr)
    return 0


def _watch_loop(args: argparse.Namespace) -> int:
    """Poll user_profile.json mtime. When it changes, trigger collection.

    Runs forever until Ctrl-C. This is the zero-touch auto-trigger: run
    once in a terminal, keep it alive, and every UI-driven profile save
    kicks off a collection.
    """
    print(
        f"info: watching {args.profile} for changes (poll={args.poll_sec}s, "
        f"Ctrl-C to stop)"
    )
    last_mtime: float | None = None
    while True:
        try:
            if os.path.exists(args.profile):
                mt = os.path.getmtime(args.profile)
                if last_mtime is None:
                    last_mtime = mt
                    print(f"info: initial mtime={mt}; waiting for change.")
                elif mt > last_mtime:
                    print(f"info: profile changed at {mt}; running collector.")
                    _run_once(args)
                    last_mtime = mt
            time.sleep(args.poll_sec)
        except KeyboardInterrupt:
            print("\ninfo: watcher stopped.")
            return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Background social-graph collector for the Warm Intro Finder."
    )
    parser.add_argument("--profile", default="user_profile.json",
                        help="Path to user_profile.json (default: %(default)s)")
    parser.add_argument("--out-dir", default=".",
                        help="Directory for people.csv / edges.csv / identities.csv / "
                             "collection_report.json / collection_progress.json "
                             "(default: %(default)s)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Use synthetic connections; do not touch CSVs.")
    parser.add_argument("--watch", action="store_true",
                        help="Poll the profile file and run collection on every change.")
    parser.add_argument("--poll-sec", type=int, default=3,
                        help="Seconds between polls in --watch mode (default: %(default)s)")
    args = parser.parse_args(argv)

    if args.watch:
        return _watch_loop(args)
    return _run_once(args)


if __name__ == "__main__":
    raise SystemExit(main())
