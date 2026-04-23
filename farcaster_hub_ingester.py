"""Farcaster public-hub → warm-intro CSVs.

BFS-crawls the Warpcast public API from a seed FID, collects at least
TARGET unique users, emits only MUTUAL follow edges (both directions
exist), and writes the engine's three-CSV format. No API key required.

Data source:
    https://api.warpcast.com/v2/following?fid=<N>&limit=100&cursor=<c>

Per-call payload: up to 100 users with fid / username / displayName /
follower+following counts. `connectedAccounts` (Twitter) and
`verifiedAddresses` (wallet) are NOT in the follows response — you get
them via /v2/user?fid=N, which we run as an optional enrichment pass.

This cut ships the fast path:
- Populate `farcaster` + `farcaster_followers` for every real user.
- Leave `twitter` / `debank` empty.
- Hit 5,000 users in ~5-10 min with --sleep 0.3.

Usage
-----
    python farcaster_hub_ingester.py --seed-fid 3 --target 5000
    python farcaster_hub_ingester.py --resume        # continue interrupted run
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
from collections import deque
from datetime import date
from pathlib import Path


BASE = "https://api.warpcast.com/v2"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)

IDENTITIES_HEADER = [
    "person_id", "twitter", "farcaster", "linkedin", "debank",
    "twitter_followers", "farcaster_followers",
    "linkedin_connections", "debank_followers",
    "last_interaction", "interaction_score",
]


def _http_get(url: str, *, timeout: float = 12.0, retries: int = 5) -> dict:
    """GET with exponential backoff on 429/503/5xx. Returns parsed JSON."""
    backoff = 1.0
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read()
                return json.loads(data.decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 502, 503, 504) and attempt < retries - 1:
                print(f"  [backoff] HTTP {e.code} - sleeping {backoff:.1f}s",
                      flush=True)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            if attempt < retries - 1:
                print(f"  [backoff] {type(e).__name__} - sleeping {backoff:.1f}s",
                      flush=True)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            raise
    raise RuntimeError(f"Retries exhausted for {url}")


def fetch_following(fid: int, *, page_size: int = 50,
                    max_follows: int = 200, sleep: float = 0.3) -> list[dict]:
    """Paginated /v2/following for one fid. Returns user dicts."""
    out: list[dict] = []
    cursor: str | None = None
    while True:
        q = {"fid": fid, "limit": page_size}
        if cursor:
            q["cursor"] = cursor
        url = f"{BASE}/following?" + urllib.parse.urlencode(q)
        data = _http_get(url)
        users = (data.get("result") or {}).get("users") or []
        out.extend(users)
        cursor = ((data.get("next") or {}).get("cursor")) if data.get("next") else None
        if len(out) >= max_follows:
            out = out[:max_follows]
            break
        if not cursor:
            break
        if sleep > 0:
            time.sleep(sleep)
    return out


def save_progress(path: Path, queue: deque, seen_following: dict,
                  profiles: dict, stats: dict) -> None:
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({
            "queue":  list(queue),
            "seen":   {str(k): sorted(v) for k, v in seen_following.items()},
            "profiles": {str(k): v for k, v in profiles.items()},
            "stats":  stats,
        }, f)
    tmp.replace(path)


def load_progress(path: Path) -> tuple[deque, dict, dict, dict]:
    with open(path, "r", encoding="utf-8") as f:
        d = json.load(f)
    return (
        deque(int(x) for x in d.get("queue") or []),
        {int(k): set(v) for k, v in (d.get("seen") or {}).items()},
        {int(k): v for k, v in (d.get("profiles") or {}).items()},
        d.get("stats") or {},
    )


def crawl(seed_fid: int, target: int, *, max_follows: int,
          sleep: float, progress_path: Path,
          resume: bool) -> tuple[dict, dict]:
    """BFS from seed_fid. Returns (profiles, seen_following).

    `target` = number of users for which we fully fetch a following-set.
    Only those users are eligible endpoints for mutual edges; everyone
    else gets discarded at emit time. This is what makes the mutual-edge
    graph dense enough to be useful.
    """
    if resume and progress_path.exists():
        queue, seen_following, profiles, stats = load_progress(progress_path)
        print(f"[resume] queue={len(queue)} profiles={len(profiles)} "
              f"seen={len(seen_following)}")
    else:
        queue = deque([seed_fid])
        seen_following: dict[int, set[int]] = {}
        profiles: dict[int, dict] = {}

    last_save = time.time()
    last_report = time.time()
    retry_count: dict[int, int] = {}
    MAX_PER_FID_RETRIES = 3

    while queue and len(seen_following) < target:
        fid = queue.popleft()
        if fid in seen_following:
            continue
        try:
            follows = fetch_following(fid, max_follows=max_follows, sleep=sleep)
        except urllib.error.HTTPError as e:
            if e.code == 404 or e.code == 403:
                # User doesn't exist or is private — mark as done-empty.
                print(f"  [skip fid={fid}] HTTP {e.code}", flush=True)
                seen_following[fid] = set()
                continue
            # Other HTTP errors: requeue with cap.
            retry_count[fid] = retry_count.get(fid, 0) + 1
            if retry_count[fid] <= MAX_PER_FID_RETRIES:
                print(f"  [requeue fid={fid}] HTTP {e.code} "
                      f"(retry {retry_count[fid]}/{MAX_PER_FID_RETRIES})",
                      flush=True)
                queue.append(fid)
                time.sleep(5.0)
            else:
                print(f"  [give-up fid={fid}] HTTP {e.code} after "
                      f"{MAX_PER_FID_RETRIES} retries", flush=True)
                seen_following[fid] = set()
            continue
        except Exception as e:
            # RuntimeError (retries exhausted), URLError, timeout, etc.
            retry_count[fid] = retry_count.get(fid, 0) + 1
            if retry_count[fid] <= MAX_PER_FID_RETRIES:
                print(f"  [requeue fid={fid}] {type(e).__name__} "
                      f"(retry {retry_count[fid]}/{MAX_PER_FID_RETRIES})",
                      flush=True)
                queue.append(fid)
                time.sleep(5.0)
            else:
                print(f"  [give-up fid={fid}] {type(e).__name__}: {e}",
                      flush=True)
                seen_following[fid] = set()
            continue

        seen_following[fid] = {u.get("fid") for u in follows if u.get("fid")}
        for u in follows:
            uf = u.get("fid")
            if not uf:
                continue
            if uf not in profiles:
                profiles[uf] = {
                    "fid": uf,
                    "username": u.get("username") or "",
                    "displayName": u.get("displayName") or "",
                    "followerCount": int(u.get("followerCount") or 0),
                    "followingCount": int(u.get("followingCount") or 0),
                }
                queue.append(uf)

        # Also make sure the seed (and any dequeued user with no entry yet)
        # has a profiles entry — otherwise emit skips them.
        if fid not in profiles:
            profiles[fid] = {
                "fid": fid, "username": "", "displayName": "",
                "followerCount": 0, "followingCount": 0,
            }

        if time.time() - last_report > 5:
            print(f"[progress] seen={len(seen_following)}/{target} "
                  f"queue={len(queue)} discovered_profiles={len(profiles)}",
                  flush=True)
            last_report = time.time()

        if time.time() - last_save > 30:
            save_progress(progress_path, queue, seen_following, profiles, {})
            last_save = time.time()

    save_progress(progress_path, queue, seen_following, profiles, {})
    return profiles, seen_following


def build_mutual_edges(seen_following: dict[int, set[int]],
                       kept: set[int]) -> list[tuple[int, int]]:
    """Return sorted-pair list of mutual-follow edges among kept FIDs."""
    edges: set[tuple[int, int]] = set()
    for a, follows_a in seen_following.items():
        if a not in kept:
            continue
        for b in follows_a:
            if b not in kept:
                continue
            # Mutual if we ALSO observed b's following set and a is in it.
            follows_b = seen_following.get(b)
            if follows_b is not None and a in follows_b:
                pair = (a, b) if a < b else (b, a)
                edges.add(pair)
    return sorted(edges)


def write_csvs(out_dir: Path, profiles: dict[int, dict],
               edges: list[tuple[int, int]]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    kept = sorted(profiles.keys())
    today = date.today().isoformat()

    with open(out_dir / "people.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id"])
        for fid in kept:
            w.writerow([f"fc_{fid}"])

    with open(out_dir / "edges.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["from", "to", "strength", "tier"])
        for a, b in edges:
            w.writerow([f"fc_{a}", f"fc_{b}", 8, "mutual"])

    with open(out_dir / "identities.csv", "w", newline="",
              encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(IDENTITIES_HEADER)
        for fid in kept:
            p = profiles[fid]
            handle = p.get("username") or ""
            if handle and not handle.startswith("@"):
                handle = f"@{handle}"
            row = [f"fc_{fid}",      # person_id
                   "",                # twitter
                   handle,            # farcaster
                   "",                # linkedin
                   "",                # debank
                   "0",                # twitter_followers
                   str(p.get("followerCount") or 0),  # farcaster_followers
                   "0",                # linkedin_connections
                   "0",                # debank_followers
                   today,              # last_interaction
                   "0"]                # interaction_score
            w.writerow(row)

    print(f"[write] {out_dir}/people.csv ({len(kept)} rows)")
    print(f"[write] {out_dir}/edges.csv ({len(edges)} rows)")
    print(f"[write] {out_dir}/identities.csv ({len(kept)} rows)")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--seed-fid", type=int, default=3,
                   help="Starting FID (default 3 = dwr.eth)")
    p.add_argument("--target", type=int, default=5000,
                   help="Stop crawl once this many unique users collected")
    p.add_argument("--max-follows-per-user", type=int, default=200,
                   help="Cap on follows fetched per user (API paging limit)")
    p.add_argument("--sleep", type=float, default=0.3,
                   help="Seconds between API calls")
    p.add_argument("--out-dir", type=Path, default=Path("farcaster_data"))
    p.add_argument("--progress-path", type=Path,
                   default=Path("collection_progress.json"))
    p.add_argument("--resume", action="store_true",
                   help="Resume from the last checkpoint if present")
    args = p.parse_args()

    print(f"[start] seed_fid={args.seed_fid} target={args.target} "
          f"max_follows={args.max_follows_per_user} sleep={args.sleep}")
    t0 = time.time()
    profiles, seen = crawl(args.seed_fid, args.target,
                           max_follows=args.max_follows_per_user,
                           sleep=args.sleep,
                           progress_path=args.progress_path,
                           resume=args.resume)
    t_crawl = time.time() - t0
    print(f"[crawl] {len(profiles)} profiles, {len(seen)} following-sets "
          f"fetched in {t_crawl:.1f}s")

    # Only emit users we fetched following for — others can't be
    # endpoints of mutual edges and would appear as isolates.
    kept = set(seen.keys())
    # Prune profiles to the kept set; ensure every kept user has a profile
    # entry (seed may not have one if it wasn't in anyone else's follows).
    for fid in kept:
        if fid not in profiles:
            profiles[fid] = {"fid": fid, "username": "", "displayName": "",
                             "followerCount": 0, "followingCount": 0}
    profiles = {fid: profiles[fid] for fid in kept}
    edges = build_mutual_edges(seen, kept)
    print(f"[mutuals] {len(edges)} mutual edges among {len(kept)} users")

    write_csvs(args.out_dir, profiles, edges)
    print(f"[done] total {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
