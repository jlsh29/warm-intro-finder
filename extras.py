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


def apply_stage(raw: dict | None, stage: int, now: str | None = None) -> dict:
    """Return a new status dict reflecting the given stage.

    Monotonic: setting stage=3 implies message_sent=True, reply_received=True,
    meeting_scheduled=True. Downward moves clear higher flags.

    Additive F9 extension: stamps `last_action_at` (ISO date) and appends to
    `stage_history` whenever the stage actually changes. Existing callers that
    don't pass `now` get today's date; existing readers ignore these fields.
    """
    stage = max(0, min(4, int(stage)))
    raw = dict(raw or {})
    prev_stage = int(raw.get("stage") or 0)
    raw["visited_profile"]   = stage >= 1 or bool(raw.get("visited_profile"))
    raw["message_sent"]      = stage >= 1
    raw["reply_received"]    = stage >= 2
    raw["meeting_scheduled"] = stage >= 3
    raw["goal_achieved"]     = stage >= 4
    raw["stage"]             = stage
    if stage != prev_stage or "last_action_at" not in raw:
        stamp = now or date.today().isoformat()
        raw["last_action_at"] = stamp
        history = list(raw.get("stage_history") or [])
        history.append({"stage": stage, "at": stamp})
        raw["stage_history"] = history
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


# -------------------------------------------------------------------------
# F9 — Dormant Bridge Surfacer
# -------------------------------------------------------------------------

def _months_between(start: date, end: date) -> float:
    return max(0.0, (end - start).days / 30.44)


def dormant_bridges(graph, profile_id: str, *, today: date | None = None,
                    limit: int = 25) -> dict:
    """Rank 1st-degree contacts by `bridge_value * months_since_last_interaction`.

    `bridge_value` = number of 2nd-degree nodes reachable through this
    contact (excluding the profile itself and the profile's other
    1st-degree contacts — those don't need an intro).
    `unique_unlocks` = subset of `bridge_value` that is reachable *only*
    through this bridge (no redundant path).
    """
    today = today or date.today()
    first_degree = set(graph.adjacency.get(profile_id, ()))
    if not first_degree:
        return {
            "bridges": [], "never_logged": [],
            "first_degree_count": 0, "max_months": 0.0,
            "total_unlocks": 0,
        }

    reach_map: dict[str, set[str]] = {}
    for b in first_degree:
        second = set()
        for nb in graph.adjacency.get(b, ()):
            if nb == profile_id or nb in first_degree:
                continue
            second.add(nb)
        reach_map[b] = second

    reach_count: Counter = Counter()
    for s_set in reach_map.values():
        for s in s_set:
            reach_count[s] += 1

    bridges: list[dict] = []
    never: list[dict] = []
    for b in first_degree:
        reach = reach_map[b]
        unique = sum(1 for s in reach if reach_count[s] == 1)
        meta = graph.id_to_meta.get(b, {}) or {}
        last_raw = (meta.get("last_interaction") or "").strip()
        last_date = _parse_date(last_raw)
        months = _months_between(last_date, today) if last_date else None
        row = {
            "id": b,
            "name": graph.id_to_name.get(b, b),
            "bridge_value": len(reach),
            "unique_unlocks": unique,
            "last_interaction": last_raw,
            "months_since": round(months, 1) if months is not None else None,
            "score": round(len(reach) * months, 1) if months is not None else None,
        }
        if months is None:
            never.append(row)
        else:
            bridges.append(row)

    bridges.sort(key=lambda r: (r["score"] or 0, r["bridge_value"]), reverse=True)
    never.sort(key=lambda r: r["bridge_value"], reverse=True)
    max_months = max((b["months_since"] or 0) for b in bridges) if bridges else 0.0
    total_unlocks = sum(b["bridge_value"] for b in bridges)
    return {
        "bridges": bridges[:limit],
        "never_logged": never[:limit],
        "first_degree_count": len(first_degree),
        "max_months": round(max_months, 1),
        "total_unlocks": total_unlocks,
    }


# -------------------------------------------------------------------------
# F10 — Outreach Tracker
# -------------------------------------------------------------------------

STAGE_ICONS = {
    0: "⚪",
    1: "📤",
    2: "⏳",
    3: "🤝",
    4: "✅",
}


def _pending_for(target_id: str, pending_store: list) -> list[dict]:
    return [m for m in (pending_store or []) if m.get("target_id") == target_id]


def _latest_message_for(target_id: str, pending_store: list) -> dict | None:
    msgs = _pending_for(target_id, pending_store)
    if not msgs:
        return None
    return max(msgs, key=lambda m: m.get("created_at") or "")


def _days_since(iso: str | None, today: date) -> int | None:
    if not iso:
        return None
    try:
        d = datetime.fromisoformat(iso.replace("Z", "+00:00")).date()
    except (ValueError, TypeError):
        d = _parse_date(iso)
    if not d:
        return None
    return max(0, (today - d).days)


def _status_color(stage: int, days_since: int | None) -> str:
    if stage >= 4:
        return "green"
    if stage == 3:
        return "blue"
    if stage in (1, 2):
        if days_since is not None and days_since >= 7:
            return "red"
        if stage == 2:
            return "yellow"
        return "blue"
    return "grey"


def _followup_tier(stage: int, days_since: int | None) -> int:
    """0 = none, 1 = 3-day gentle, 2 = 7-day different angle, 3 = 14-day last attempt."""
    if stage >= 3 or days_since is None:
        return 0
    if days_since >= 14:
        return 3
    if days_since >= 7:
        return 2
    if days_since >= 3:
        return 1
    return 0


def tracker_rows(graph, outreach_store: dict, pending_store: list,
                 today: date | None = None) -> list[dict]:
    """One row per target with any outreach activity (stage >= 1 OR pending msg).

    Each row contains everything the tracker UI needs. Handle/account
    enrichment is done by the app.py route before rendering.
    """
    today = today or date.today()
    target_ids: set[str] = set()
    for tid, raw in (outreach_store or {}).items():
        if int((raw or {}).get("stage") or 0) >= 1 or (raw or {}).get("message_sent"):
            target_ids.add(tid)
    for msg in pending_store or []:
        tid = msg.get("target_id")
        if tid:
            target_ids.add(tid)

    rows = []
    for tid in target_ids:
        raw = (outreach_store or {}).get(tid) or {}
        stage = stage_from_raw(raw)
        latest_msg = _latest_message_for(tid, pending_store)
        last_action_at = raw.get("last_action_at")
        if not last_action_at and latest_msg:
            last_action_at = (latest_msg.get("created_at") or "")[:10]
        days_since = _days_since(last_action_at, today)
        tier = _followup_tier(stage, days_since)
        rows.append({
            "target_id": tid,
            "name": graph.id_to_name.get(tid, tid),
            "stage": stage,
            "stage_label": STAGE_LABELS.get(stage, ""),
            "stage_icon": STAGE_ICONS.get(stage, ""),
            "status_color": _status_color(stage, days_since),
            "last_action_at": last_action_at or "",
            "days_since": days_since if days_since is not None else -1,
            "latest_message": (latest_msg or {}).get("message", ""),
            "latest_platform": (latest_msg or {}).get("platform", ""),
            "followup_tier": tier,
            "stage_history": list(raw.get("stage_history") or []),
            "message_count": len(_pending_for(tid, pending_store)),
        })
    return rows


def tracker_stats(outreach_store: dict, pending_store: list,
                  today: date | None = None) -> dict:
    """Aggregate tracker-wide stats: counts, success rate, avg reply days, best platform, stage distribution."""
    today = today or date.today()
    base = outreach_stats(outreach_store)
    stage_dist = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
    reply_days: list[int] = []
    for tid, raw in (outreach_store or {}).items():
        if not (raw or {}).get("message_sent"):
            continue
        s = stage_from_raw(raw)
        stage_dist[s] = stage_dist.get(s, 0) + 1
        # Compute days from first message to reply for targets that replied.
        if s >= 2:
            history = raw.get("stage_history") or []
            sent_at = next((h["at"] for h in history if h["stage"] == 1), None)
            replied_at = next((h["at"] for h in history if h["stage"] == 2), None)
            if sent_at and replied_at:
                d_sent = _parse_date(sent_at)
                d_rep = _parse_date(replied_at)
                if d_sent and d_rep:
                    reply_days.append(max(0, (d_rep - d_sent).days))
                    continue
            # Fallback: use latest pending message created_at → now.
            latest = _latest_message_for(tid, pending_store)
            if latest:
                d_sent = _parse_date((latest.get("created_at") or "")[:10])
                if d_sent:
                    reply_days.append(max(0, (today - d_sent).days))

    # Best platform = platform with most goal_achieved wins.
    platform_wins: Counter = Counter()
    for tid, raw in (outreach_store or {}).items():
        if not (raw or {}).get("goal_achieved"):
            continue
        latest = _latest_message_for(tid, pending_store)
        if latest and latest.get("platform"):
            platform_wins[latest["platform"]] += 1
    best_platform = platform_wins.most_common(1)[0][0] if platform_wins else "—"

    avg_reply = round(sum(reply_days) / len(reply_days), 1) if reply_days else 0.0
    return {
        **base,
        "avg_days_to_reply": avg_reply,
        "best_platform": best_platform,
        "stage_distribution": stage_dist,
    }


_FOLLOWUP_TEMPLATES = {
    1: ("Gentle reminder",
        "Hi {target}, just wanted to follow up on my previous message. I came across your profile through {mutual} and would love to connect!"),
    2: ("Different angle",
        "Hi {target}, I noticed we're both connected with {mutual}. I wanted to reach out because I admire your work — would love to compare notes when you have a moment."),
    3: ("Last attempt",
        "Hi {target}, I understand you're busy so I'll keep this brief — I wanted to connect because of our mutual {mutual}. Would love to chat if you ever have 15 minutes."),
}


def followup_template(tier: int, target_handle: str = "@target",
                      mutual_handle: str = "@mutual") -> dict:
    label, body = _FOLLOWUP_TEMPLATES.get(tier, ("Follow-up", "Hi {target}, following up here — hope you're well!"))
    return {
        "tier": tier,
        "label": label,
        "message": body.format(target=target_handle, mutual=mutual_handle),
    }


def reconnect_suggestions(graph, profile_id: str, outreach_store: dict,
                          pending_store: list, today: date | None = None,
                          limit: int = 5) -> list[dict]:
    """People the user hasn't reached out to but probably should.

    Excludes anyone already at stage >= 1 or with any pending message.
    Ranks by (interaction_score + bridge_value/5) * days_since_last_interaction.
    """
    today = today or date.today()
    excluded: set[str] = {
        tid for tid, raw in (outreach_store or {}).items()
        if int((raw or {}).get("stage") or 0) >= 1 or (raw or {}).get("message_sent")
    }
    for m in pending_store or []:
        tid = m.get("target_id")
        if tid:
            excluded.add(tid)

    direct = set(graph.adjacency.get(profile_id, ()))
    candidates = []
    for node_id in direct:
        if node_id in excluded or node_id == profile_id:
            continue
        meta = graph.id_to_meta.get(node_id, {}) or {}
        try:
            score = int(meta.get("interaction_score") or 0)
        except (ValueError, TypeError):
            score = 0
        last_raw = (meta.get("last_interaction") or "").strip()
        last_date = _parse_date(last_raw)
        days_since = (today - last_date).days if last_date else 365
        if days_since < 30:
            continue  # Spec: "last interaction was 30+ days ago"
        # How many 2nd-degree contacts this person bridges to.
        bridge_value = 0
        for nb in graph.adjacency.get(node_id, ()):
            if nb != profile_id and nb not in direct:
                bridge_value += 1
        rank = (score + bridge_value / 5.0) * days_since
        candidates.append({
            "id": node_id,
            "name": graph.id_to_name.get(node_id, node_id),
            "interaction_score": score,
            "last_interaction": last_raw,
            "days_since": days_since,
            "connects_to_count": bridge_value,
            "bridge_value": bridge_value,
            "rank_score": round(rank, 1),
            "reason": f"Connected to {bridge_value} people you might want to reach",
        })

    candidates.sort(key=lambda r: r["rank_score"], reverse=True)
    return candidates[:limit]

