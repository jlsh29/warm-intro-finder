"""Helpers for the 8 add-on features (AI generator, leaderboard, success
tracker, best-time, gap finder, influence, path comparison, weekly report).

Kept separate from `app.py` so route handlers stay small and the existing
graph / path-finding modules (`core.py`, `warm_intro.py`, `identity.py`)
stay untouched — every stress test works against those, not this module.

No new third-party deps: Anthropic API is called via `urllib.request`.
"""

from __future__ import annotations

import json
import os
from collections import Counter
from datetime import date, datetime, timedelta
from urllib import error as urlerror
from urllib import request as urlrequest


# -------------------------------------------------------------------------
# F1 — Anthropic client (stdlib only, no `requests` dep required)
# -------------------------------------------------------------------------

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = "claude-sonnet-4-20250514"
ANTHROPIC_VERSION = "2023-06-01"


def anthropic_key_present() -> bool:
    return bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())


def anthropic_complete(system: str, user: str, *, max_tokens: int = 400,
                       timeout: int = 15) -> str:
    """POST to Anthropic /v1/messages and return the first text block.

    Raises RuntimeError on key missing or any HTTP / payload problem.
    """
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not key:
        raise RuntimeError("ANTHROPIC_API_KEY is not set")
    body = json.dumps({
        "model":      ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        "system":     system,
        "messages":   [{"role": "user", "content": user}],
    }).encode("utf-8")
    req = urlrequest.Request(
        ANTHROPIC_URL,
        data=body,
        headers={
            "content-type":      "application/json",
            "x-api-key":         key,
            "anthropic-version": ANTHROPIC_VERSION,
        },
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urlerror.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
        raise RuntimeError(f"Anthropic HTTP {e.code}: {detail[:300]}")
    except urlerror.URLError as e:
        raise RuntimeError(f"Anthropic network error: {e.reason}")
    payload = json.loads(raw)
    blocks = payload.get("content") or []
    for b in blocks:
        if b.get("type") == "text" and b.get("text"):
            return b["text"].strip()
    raise RuntimeError("Anthropic returned no text content")


def generate_intro_message(
    *,
    sender_name: str,
    sender_handle: str,
    mutual_name: str | None,
    mutual_handle: str | None,
    target_name: str,
    target_handle: str,
    platform_label: str,
    interaction_summary: str,
) -> str:
    system = (
        "You are a concise outreach coach. You write short, warm, natural "
        "intro messages (2-4 sentences). Never use emojis or hashtags. "
        "Never invent facts about the recipient. Match the casual, friendly "
        "tone of someone reaching out with a real mutual."
    )
    mutual_clause = (
        f"They share a mutual connection, {mutual_handle or mutual_name}."
        if mutual_name
        else "They have no direct mutual — write a polite cold-style opener."
    )
    user = (
        f"Write a warm intro message.\n"
        f"- Sender: {sender_name} ({sender_handle})\n"
        f"- Recipient: {target_name} ({target_handle})\n"
        f"- Platform: {platform_label}\n"
        f"- {mutual_clause}\n"
        f"- Interaction context: {interaction_summary or 'light context'}\n\n"
        f"Return ONLY the message body (no greeting prefix like 'Message:')."
    )
    return anthropic_complete(system, user)


# -------------------------------------------------------------------------
# Helper
# -------------------------------------------------------------------------

def _int_attr(meta: dict | None, key: str, default: int = 0) -> int:
    try:
        return int((meta or {}).get(key, default) or default)
    except (ValueError, TypeError):
        return default


# -------------------------------------------------------------------------
# F3 — Success tracker pipeline (stages 0..4)
# -------------------------------------------------------------------------

STAGE_LABELS = {
    0: "Not Started",
    1: "Message Sent",
    2: "Awaiting Reply",
    3: "Meeting Scheduled",
    4: "Goal Achieved",
}


def stage_from_raw(raw: dict | None) -> int:
    raw = raw or {}
    if raw.get("goal_achieved"):
        return 4
    if raw.get("meeting_scheduled"):
        return 3
    if raw.get("reply_received"):
        return 2
    if raw.get("message_sent"):
        return 1
    return 0


def apply_stage(raw: dict | None, stage: int) -> dict:
    """Return a new status dict reflecting the given stage.

    Monotonic: setting stage=3 implies message_sent=True, reply_received=True,
    meeting_scheduled=True. Downward moves clear higher flags.
    """
    stage = max(0, min(4, int(stage)))
    raw = dict(raw or {})
    raw["visited_profile"]   = stage >= 1 or bool(raw.get("visited_profile"))
    raw["message_sent"]      = stage >= 1
    raw["reply_received"]    = stage >= 2
    raw["meeting_scheduled"] = stage >= 3
    raw["goal_achieved"]     = stage >= 4
    raw["stage"]             = stage
    return raw


def outreach_stats(all_status: dict) -> dict:
    """Aggregate counts + percentages for the dashboard."""
    total = sum(1 for v in all_status.values() if (v or {}).get("message_sent"))
    replies  = sum(1 for v in all_status.values() if (v or {}).get("reply_received"))
    meetings = sum(1 for v in all_status.values() if (v or {}).get("meeting_scheduled"))
    wins     = sum(1 for v in all_status.values() if (v or {}).get("goal_achieved"))

    def pct(n: int) -> float:
        return round((n / total) * 100, 1) if total else 0.0

    return {
        "total_outreach":  total,
        "reply_count":     replies,
        "meeting_count":   meetings,
        "success_count":   wins,
        "reply_rate":      pct(replies),
        "meeting_rate":    pct(meetings),
        "success_rate":    pct(wins),
    }


# -------------------------------------------------------------------------
# F4 — Best time to reach out
# -------------------------------------------------------------------------

WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday",
                 "Friday", "Saturday", "Sunday"]


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


_PLATFORM_DISPLAY = {
    "twitter":   "Twitter",
    "farcaster": "Farcaster",
    "linkedin":  "LinkedIn",
    "wallet":    "DeBank",
}


def best_time(
    target_id: str,
    graph,
    *,
    target_handle: str | None = None,
    active_platforms: set[str] | None = None,
) -> dict:
    """Heuristic best-time-to-reach-out with human-readable reasoning.

    Returns a dict consumed by the template:
      {
        label: "Best time: ...",
        reason: "Based on N interactions, @handle is most active on ...",
        platform: "twitter",
        platform_label: "Twitter",
        interactions: int,
        per_platform: {platform: str, ...},
        empty: bool,
      }
    """
    accounts = [a for a in graph.accounts if a.owner_person_id == target_id]
    # BUG2 — scope everything to the entry person's platforms, if supplied.
    if active_platforms is not None:
        accounts = [a for a in accounts if a.platform in active_platforms]
    meta = graph.id_to_meta.get(target_id, {}) or {}
    last = _parse_date(meta.get("last_interaction"))
    handle = (target_handle or "").strip() or graph.id_to_name.get(target_id, target_id)

    per_platform_weight: Counter[str] = Counter()
    for acc in accounts:
        w = 0
        for v in (acc.attributes or {}).values():
            try:
                w += int(v or 0)
            except (ValueError, TypeError):
                continue
        per_platform_weight[acc.platform] += w
    total_interactions = sum(per_platform_weight.values())

    # Fallback when we have no signal at all.
    if total_interactions == 0 and last is None:
        return {
            "label": "No interaction history yet.",
            "reason": (
                "Generally, weekday mornings (9-11am) and evenings (7-9pm) "
                "have the highest response rates on social media."
            ),
            "platform": None,
            "platform_label": None,
            "interactions": 0,
            "per_platform": {},
            "empty": True,
        }

    # Weekday vs weekend cue from last_interaction date.
    if last and last.weekday() >= 5:
        is_weekend = True
        day_bucket = "weekends"
    else:
        is_weekend = False
        day_bucket = "weekdays"

    top_platform = ""
    if per_platform_weight:
        top_platform = per_platform_weight.most_common(1)[0][0]
    top_label = _PLATFORM_DISPLAY.get(top_platform, top_platform.title() or "multiple platforms")

    if is_weekend:
        window = "Weekends 10am-12pm or 4pm-6pm"
        time_phrase = "weekend late-morning / late-afternoon"
    else:
        if top_platform == "linkedin":
            window = "Weekdays 9am-11am"
            time_phrase = "morning business hours on weekdays"
        elif top_platform in ("twitter", "farcaster"):
            window = "Weekdays 9am-11am or Evenings 7pm-9pm"
            time_phrase = "morning hours on weekdays"
        elif top_platform == "wallet":
            window = "Evenings 6pm-10pm"
            time_phrase = "evenings (on-chain activity window)"
        else:
            window = "Weekdays 9am-11am or Evenings 7pm-9pm"
            time_phrase = "mornings and evenings on weekdays"

    reason = (
        f"Based on {total_interactions} interaction{'' if total_interactions == 1 else 's'}, "
        f"{handle} is most active on {top_label} during {time_phrase}."
    )
    if last:
        reason += f" Last recorded interaction: {last.strftime('%A, %Y-%m-%d')}."

    per_platform: dict[str, str] = {}
    for p in per_platform_weight:
        if p == "linkedin":
            per_platform[p] = "Tue-Thu 9am-11am"
        elif p == "twitter":
            per_platform[p] = "Weekdays 8am-10am or 8pm-10pm"
        elif p == "farcaster":
            per_platform[p] = "Evenings 7pm-10pm"
        elif p == "wallet":
            per_platform[p] = "Evenings 6pm-10pm"

    return {
        "label": f"Best time: {window}",
        "reason": reason,
        "platform": top_platform or None,
        "platform_label": top_label if top_platform else None,
        "interactions": total_interactions,
        "per_platform": per_platform,
        "empty": False,
        "last_day": WEEKDAY_NAMES[last.weekday()] if last else day_bucket,
    }


# -------------------------------------------------------------------------
# F6 — Influence score (0-100)
# -------------------------------------------------------------------------

def _max_connections(graph, profile_id: str) -> int:
    best = 1
    for pid in graph.id_to_name:
        if pid == profile_id:
            continue
        c = len(graph.adjacency.get(pid, set()))
        if c > best:
            best = c
    return best


def influence_score(node_id: str, graph, *, profile_id: str = "me",
                    max_connections_hint: int | None = None) -> int:
    """Composite 0-100 influence score.

    0.40 * connections_norm
    0.35 * avg_interaction_score_norm (self score, 0-15)
    0.25 * platform_coverage_norm (1-4 platforms)
    """
    if node_id == profile_id:
        # Self gets a reasonable default so it renders a badge.
        return 100
    max_conn = max_connections_hint or _max_connections(graph, profile_id)
    conns = len(graph.adjacency.get(node_id, set()))
    connections_norm = min(1.0, conns / max_conn) if max_conn else 0.0

    meta = graph.id_to_meta.get(node_id, {}) or {}
    score = _int_attr(meta, "interaction_score", 0)
    interaction_norm = min(1.0, score / 15.0)

    platforms = {acc.platform for acc in graph.accounts if acc.owner_person_id == node_id}
    platform_norm = min(1.0, len(platforms) / 4.0)

    raw = 0.40 * connections_norm + 0.35 * interaction_norm + 0.25 * platform_norm
    return int(round(raw * 100))

