"""Enrich the Farcaster hub dataset with Twitter handles + primary ETH wallet.

Reads `farcaster_data/people.csv` for the kept FIDs, hits
  * /v2/user?fid=N          -> connectedAccounts (platform='x' -> Twitter)
  * /v2/verifications?fid=N -> primary eth address

Updates `farcaster_data/identities.csv` in place (rewrites the file with
the enriched `twitter` and `debank` columns populated).

Idempotent: reads existing identities.csv, skips any row that already has
both twitter and debank populated. Writes a checkpoint file so Ctrl-C is
safe — re-run to continue.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


BASE = "https://api.warpcast.com/v2"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)


def _get(url: str, retries: int = 6, timeout: float = 15.0) -> dict | None:
    backoff = 1.0
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": UA, "Accept": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code in (429, 502, 503, 504) and attempt < retries - 1:
                print(f"  [backoff] HTTP {e.code} sleeping {backoff:.1f}s", flush=True)
                time.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
                continue
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            if attempt < retries - 1:
                print(f"  [backoff] {type(e).__name__} sleeping {backoff:.1f}s",
                      flush=True)
                time.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
                continue
            raise
    return None


def fetch_enrichment(fid: int) -> tuple[str, str]:
    """Return (twitter_handle, eth_address). Either may be empty."""
    twitter = ""
    wallet = ""
    u = _get(f"{BASE}/user?fid={fid}")
    if u:
        user = (u.get("result") or {}).get("user") or {}
        for acc in user.get("connectedAccounts") or []:
            if (acc.get("platform") or "").lower() == "x" and not acc.get("expired"):
                h = (acc.get("username") or "").lstrip("@").strip()
                if h:
                    twitter = f"@{h}"
                    break
    v = _get(f"{BASE}/verifications?fid={fid}")
    if v:
        primary_eth = ""
        fallback_eth = ""
        for ver in (v.get("result") or {}).get("verifications") or []:
            if (ver.get("protocol") or "").lower() != "ethereum":
                continue
            addr = (ver.get("address") or "").strip()
            if not addr:
                continue
            if ver.get("isPrimary") and not primary_eth:
                primary_eth = addr
            elif not fallback_eth:
                fallback_eth = addr
        wallet = primary_eth or fallback_eth
    return twitter, wallet


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--identities", type=Path,
                   default=Path("farcaster_data/identities.csv"))
    p.add_argument("--people", type=Path,
                   default=Path("farcaster_data/people.csv"))
    p.add_argument("--sleep", type=float, default=0.4,
                   help="Seconds between API call pairs")
    p.add_argument("--limit", type=int, default=0,
                   help="Max rows to enrich this run (0 = all)")
    p.add_argument("--save-every", type=int, default=50)
    args = p.parse_args()

    # Load existing identities so we can update in place.
    rows: list[dict] = []
    with open(args.identities, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        for r in reader:
            rows.append(r)
    if not fieldnames:
        print("[error] identities.csv missing header", file=sys.stderr)
        return 1

    # Filter to rows that still need enrichment.
    todo_idx: list[int] = []
    for i, r in enumerate(rows):
        # Only farcaster-sourced rows start with fc_
        if not (r.get("person_id") or "").startswith("fc_"):
            continue
        if (r.get("twitter") or "").strip() and (r.get("debank") or "").strip():
            continue
        todo_idx.append(i)

    total = len(todo_idx)
    print(f"[enrich] {total} rows need enrichment (of {len(rows)} total)")
    if args.limit and args.limit > 0:
        todo_idx = todo_idx[:args.limit]
        print(f"[enrich] limiting to first {len(todo_idx)} this run")

    def _save() -> None:
        tmp = args.identities.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
        tmp.replace(args.identities)

    t0 = time.time()
    done = 0
    hit_twitter = 0
    hit_wallet = 0
    for n, i in enumerate(todo_idx, 1):
        r = rows[i]
        pid = r["person_id"]
        try:
            fid = int(pid.split("_", 1)[1])
        except (ValueError, IndexError):
            continue
        try:
            tw, wal = fetch_enrichment(fid)
        except Exception as e:
            print(f"  [skip fid={fid}] {type(e).__name__}: {e}", flush=True)
            tw, wal = "", ""
        if tw:
            r["twitter"] = tw
            hit_twitter += 1
        if wal:
            r["debank"] = wal
            hit_wallet += 1
        done += 1
        if n % 25 == 0 or n == len(todo_idx):
            dt = time.time() - t0
            rate = done / max(dt, 0.001)
            eta = (len(todo_idx) - n) / max(rate, 0.001)
            print(f"[enrich] {n}/{len(todo_idx)} "
                  f"twitter={hit_twitter} wallet={hit_wallet} "
                  f"rate={rate:.2f}/s eta={eta/60:.1f}min",
                  flush=True)
        if n % args.save_every == 0:
            _save()
        if args.sleep > 0:
            time.sleep(args.sleep)

    _save()
    print(f"[done] enriched {done} rows: "
          f"twitter_hits={hit_twitter} wallet_hits={hit_wallet} "
          f"elapsed={time.time()-t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
