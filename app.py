"""Flask UI for the warm-intro path finder.

Run:
    python app.py
Then open http://127.0.0.1:5000/

Current feature set:
  * Single entry point: the user profile stored in `user_profile.json`
    (editable from the banner at the top of the page).
  * Free-text target search across every person in the network (1st,
    2nd, 3rd degrees) with autocomplete suggestions that show degree.
  * Top-3 ranked paths, chosen from a wider Yen's candidate pool by a
    composite score: 40% hop-count, 35% mutual-platform count, 25%
    connection strength.
  * Step-by-step Option A outreach (one message per hop, each
    referencing the previous introducer) + Option B (direct DM per
    platform the target is on).
  * Mutual-platform status panel per hop.

All pathfinding and graph logic lives in warm_intro.py / core.py. This
module composes the view, nothing more. `run_stress_tests.py` never
touches the Flask layer, so the legacy `find_warm_intro` stays intact.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from urllib.parse import quote


def _load_env_file(path: str = ".env") -> None:
    """Minimal stdlib .env loader so `ANTHROPIC_API_KEY` (and friends)
    flow into `os.environ` without requiring python-dotenv. Existing env
    vars take precedence — we never overwrite values already set by the
    shell.
    """
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip()
                # Strip surrounding single/double quotes if present.
                if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                    val = val[1:-1]
                if key and key not in os.environ:
                    os.environ[key] = val
    except OSError:
        pass


_load_env_file()


from flask import Flask, jsonify, redirect, render_template, request, url_for

from seed_profile_network import (
    PROFILE_ID,
    default_profile,
    handle_key,
    seed_for_profile,
)
from warm_intro import build_graph, find_ranked_paths, yen_k_shortest_paths

import extras

PEOPLE = os.environ.get("WARM_INTRO_PEOPLE", "people.csv")
EDGES = os.environ.get("WARM_INTRO_EDGES", "edges.csv")
IDENTITIES = os.environ.get("WARM_INTRO_IDENTITIES", "identities.csv")
USER_PROFILE_PATH = os.environ.get("WARM_INTRO_PROFILE", "user_profile.json")
OUTREACH_STATUS_PATH = os.environ.get(
    "WARM_INTRO_OUTREACH_STATUS", "outreach_status.json"
)
PENDING_MESSAGES_PATH = os.environ.get(
    "WARM_INTRO_PENDING_MESSAGES", "pending_messages.json"
)

TOP_K = 3                       # product spec
CANDIDATE_POOL = 10             # Yen's candidates to consider before re-ranking
MAX_DEGREE_LABEL = 99           # fallback when BFS can't reach a node

MEDALS: dict[int, tuple[str, str]] = {
    1: ("🥇", "BEST PATH"),
    2: ("🥈", "GOOD PATH"),
    3: ("🥉", "POSSIBLE PATH"),
}

PLATFORM_LABEL: dict[str, str] = {
    "twitter":   "Twitter / X",
    "farcaster": "Farcaster",
    "linkedin":  "LinkedIn",
    "wallet":    "DeBank",
}

PLATFORMS = ("twitter", "farcaster", "linkedin", "wallet")

# Interaction icons for real, API-verified interaction data only.
# Fake columns (twitter_likes, twitter_comments, twitter_reposts,
# farcaster_recasts, farcaster_replies, linkedin_recommendations,
# linkedin_endorsements, debank_transactions) have been removed.
# Show nothing rather than synthetic numbers.
INTERACTION_ICONS: list[dict] = []


def strength_bucket(s: float | int) -> dict:
    """Categorise an edge strength for the UI label/colour on the arrow.

    Buckets match the spec: 12-15 Very Strong (green), 8-11 Strong
    (blue), 4-7 Moderate (yellow), 1-3 Weak (grey).
    """
    v = int(s or 0)
    if v >= 12:
        return {"label": "Very Strong", "emoji": "🔥", "band": "very-strong"}
    if v >= 8:
        return {"label": "Strong",      "emoji": "💪", "band": "strong"}
    if v >= 4:
        return {"label": "Moderate",    "emoji": "👋", "band": "moderate"}
    return {"label": "Weak", "emoji": "🤝", "band": "weak"}

app = Flask(__name__)


# ---- profile persistence ------------------------------------------------

def load_profile() -> dict:
    if not os.path.exists(USER_PROFILE_PATH):
        return {}
    try:
        with open(USER_PROFILE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except (json.JSONDecodeError, OSError):
        pass
    return {}


def save_profile(profile: dict) -> None:
    with open(USER_PROFILE_PATH, "w", encoding="utf-8") as f:
        json.dump(profile, f, indent=2)


def _profile_has_handles(profile: dict | None) -> bool:
    """True iff the profile owns at least one social handle."""
    p = profile or {}
    return any((p.get(k) or "").strip()
               for k in ("twitter", "farcaster", "linkedin", "debank"))


_PROFILE_TO_PLATFORM = {
    "twitter":   "twitter",
    "farcaster": "farcaster",
    "linkedin":  "linkedin",
    "debank":    "wallet",
}


def _active_platforms() -> set[str]:
    """Platforms the current profile owns — used to scope every per-platform
    UI surface (interaction chips, mutual panel, best-time grid, outreach
    tabs) to the entry person's actual platforms."""
    prof = load_profile() or {}
    out: set[str] = set()
    for field, platform in _PROFILE_TO_PLATFORM.items():
        if (prof.get(field) or "").strip():
            out.add(platform)
    return out


def _clear_dataset() -> None:
    """Write header-only CSVs so build_graph() yields an empty graph.

    Used when the user saves a profile with no social handles — the prior
    seeded dataset is replaced with nothing to represent the empty state.
    Headers match what `core.CSVRepository` expects.
    """
    with open(PEOPLE, "w", encoding="utf-8", newline="") as f:
        f.write("id,name\n")
    with open(EDGES, "w", encoding="utf-8", newline="") as f:
        f.write("from,to,tier,strength,source\n")
    with open(IDENTITIES, "w", encoding="utf-8", newline="") as f:
        f.write("person_id,twitter,farcaster,linkedin,debank\n")


def _ensure_dataset(profile: dict) -> None:
    # Empty profile → never seed. Leave whatever is on disk alone; any
    # explicit profile-save event (or the fall-through default profile at
    # first run) is what populates the dataset.
    if not _profile_has_handles(profile):
        return
    if not (
        os.path.exists(PEOPLE)
        and os.path.exists(EDGES)
        and os.path.exists(IDENTITIES)
    ):
        seed_for_profile(profile)


# ---- graph bootstrap ----------------------------------------------------

profile = load_profile() or default_profile()
_ensure_dataset(profile)
graph = build_graph(PEOPLE, EDGES, identities_path=IDENTITIES)


def rebuild_graph() -> None:
    global graph
    graph = build_graph(PEOPLE, EDGES, identities_path=IDENTITIES)


# ---- URL + social helpers ----------------------------------------------

def link_for(platform: str, handle: str) -> str | None:
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
    if p == "wallet":
        return f"https://debank.com/profile/{h}"
    return None


def compose_url_for(platform: str, handle: str) -> str | None:
    """Deep-link opening a DM compose/profile page on the target platform."""
    h = (handle or "").strip().lstrip("@")
    if not h:
        return None
    p = (platform or "").strip().lower()
    if p == "twitter":
        return f"https://twitter.com/messages/compose?recipient={quote(h)}"
    if p == "linkedin":
        if handle.startswith("http"):
            return handle
        return f"https://linkedin.com/in/{h}"
    if p == "farcaster":
        return f"https://warpcast.com/{h}"
    if p == "wallet":
        return f"https://debank.com/profile/{h}"
    return None


def accounts_for(node_id: str) -> list[dict]:
    out: list[dict] = []
    active = _active_platforms()
    for acc in graph.accounts:
        if acc.owner_person_id != node_id:
            continue
        # BUG2 — only surface platforms the entry person (profile) has.
        if acc.platform not in active:
            continue
        url = link_for(acc.platform, acc.handle)
        if not url:
            continue
        dm_raw = (acc.attributes.get("dm") or "").strip().lower()
        dm: str | None = None
        if dm_raw in ("yes", "y", "true", "1"):
            dm = "yes"
        elif dm_raw in ("no", "n", "false", "0"):
            dm = "no"
        # Copy per-platform interaction counts from SocialAccount.attributes.
        interactions: dict[str, int] = {}
        for spec in INTERACTION_ICONS:
            if spec["platform"] != acc.platform:
                continue
            raw = acc.attributes.get(spec["key"], "0")
            try:
                interactions[spec["key"]] = int(raw or 0)
            except (ValueError, TypeError):
                interactions[spec["key"]] = 0
        out.append({
            "platform": acc.platform,
            "handle": acc.handle,
            "url": url,
            "dm": dm,
            "interactions": interactions,
        })
    return out


def _person_attributes(node_id: str) -> dict:
    """Raw person.attributes dict, for surfacing interaction_score etc."""
    for cluster in graph.identity_clusters:
        if cluster.person_id == node_id:
            pass  # not used directly; Person attributes are in graph.id_to_meta
    return graph.id_to_meta.get(node_id, {}) or {}


def _interaction_summary(accounts: list[dict]) -> list[dict]:
    """Flatten every >0 interaction count across the account list, in
    the stable INTERACTION_ICONS order, so the template can iterate
    just the keys to render on the card (only non-zero shown)."""
    summary: list[dict] = []
    for spec in INTERACTION_ICONS:
        total = 0
        for acc in accounts:
            if acc["platform"] != spec["platform"]:
                continue
            total += acc.get("interactions", {}).get(spec["key"], 0)
        if total > 0:
            summary.append({
                "key": spec["key"],
                "platform": spec["platform"],
                "icon": spec["icon"],
                "label": spec["label"],
                "count": total,
            })
    return summary


def _platforms_of(node_id: str) -> set[str]:
    return {acc["platform"] for acc in accounts_for(node_id)}


def _handle_display(account: dict | None) -> str:
    if not account:
        return ""
    h = account.get("handle", "")
    p = account.get("platform", "")
    if p in ("twitter", "farcaster"):
        return h if h.startswith("@") else f"@{h}"
    return h


def _preferred_handle(node_id: str) -> str:
    """Return a human-friendly handle to address the person in a message.

    Prefers @twitter → @farcaster → LinkedIn slug → wallet. Falls back
    to the display name if no accounts exist.
    """
    accs = {a["platform"]: a for a in accounts_for(node_id)}
    for p in ("twitter", "farcaster", "linkedin", "wallet"):
        if p in accs:
            return _handle_display(accs[p])
    return graph.id_to_name.get(node_id, node_id)


def node_view(node_id: str, *, degree_map: dict[str, int] | None = None) -> dict:
    d = degree_map.get(node_id) if degree_map else None
    accs = accounts_for(node_id)
    attrs = _person_attributes(node_id)
    try:
        score = int(attrs.get("interaction_score", 0) or 0)
    except (ValueError, TypeError):
        score = 0
    last = attrs.get("last_interaction", "")
    # F6 — influence score (additive; legacy templates ignore).
    try:
        influence = extras.influence_score(node_id, graph, profile_id=PROFILE_ID)
    except Exception:
        influence = 0
    view = {
        "id": node_id,
        "name": graph.id_to_name.get(node_id, node_id),
        "accounts": accs,
        "handle": _preferred_handle(node_id),
        "degree": d,
        # New interaction fields (additive — older templates just ignore).
        "interactions": _interaction_summary(accs),
        "interaction_score": score,
        "last_interaction": last,
        "influence": influence,
    }
    # JSON-safe payload pre-built for the interaction-details modal
    # (Jinja can't easily compose nested dict literals with loops).
    view["modal_payload"] = {
        "id": node_id,
        "name": view["name"],
        "handle": view["handle"],
        "interaction_score": score,
        "last_interaction": last,
        "accounts": [
            {"platform": a["platform"], "handle": a["handle"],
             "interactions": a.get("interactions", {})}
            for a in accs
        ],
    }
    return view


# ---- degrees (BFS from PROFILE_ID) ------------------------------------

def compute_degrees() -> dict[str, int]:
    """BFS distances from `me` to every reachable node, capped at 3."""
    if PROFILE_ID not in graph.id_to_name:
        return {}
    dist: dict[str, int] = {PROFILE_ID: 0}
    q: deque[str] = deque([PROFILE_ID])
    while q:
        n = q.popleft()
        d = dist[n]
        for nb in graph.adjacency.get(n, ()):
            if nb not in dist:
                dist[nb] = d + 1
                q.append(nb)
    return dist


def degree_label(d: int | None) -> str:
    if d is None:
        return "Unreached"
    if d == 0:
        return "You"
    if d == 1:
        return "1st degree"
    if d == 2:
        return "2nd degree"
    if d == 3:
        return "3rd degree"
    return f"{d}th degree"


# ---- mutual-platform status per hop ----------------------------------

def mutual_status(a_id: str, b_id: str) -> list[dict]:
    """Mutual-platform status between two nodes, scoped to the entry
    person's platforms only (BUG2)."""
    a = _platforms_of(a_id)
    b = _platforms_of(b_id)
    active = _active_platforms()
    return [
        {
            "platform": p,
            "platform_label": PLATFORM_LABEL[p],
            "a_has": p in a,
            "b_has": p in b,
            "mutual": (p in a) and (p in b),
        }
        for p in PLATFORMS if p in active
    ]


def _mutual_count(a_id: str, b_id: str) -> int:
    return sum(1 for m in mutual_status(a_id, b_id) if m["mutual"])


# ---- outreach (single direct-to-target message per platform) ---------

def _discovery_platform(mutual_id: str, target_id: str) -> tuple[str | None, str]:
    """Platform on which the mutual and target are both present.

    Used by outreach messages as "I came across your profile through our
    mutual connection @X on {discovery_platform}." Picks a stable order
    so the same path always yields the same phrasing.
    """
    active = _active_platforms()
    shared = [
        p for p in PLATFORMS
        if p in active
        and p in _platforms_of(mutual_id)
        and p in _platforms_of(target_id)
    ]
    if not shared:
        return None, ""
    p = shared[0]
    return p, PLATFORM_LABEL.get(p, p)


def _build_direct_outreach(nodes: list[dict], target_node: dict) -> list[dict]:
    """One message per platform the target is on. The message weaves in
    the immediate mutual (nodes[-2]) and the discovery platform so the
    target knows where the thread came from. A 1-hop path (no mutual)
    degrades to a generic "came across your profile" phrasing."""
    out: list[dict] = []
    mutual = nodes[-2] if len(nodes) >= 2 else None
    discovery_platform_label = ""
    if mutual:
        _, discovery_platform_label = _discovery_platform(mutual["id"], target_node["id"])

    for acc in target_node["accounts"]:
        compose = compose_url_for(acc["platform"], acc["handle"])
        if not compose:
            continue
        target_handle = _handle_display(acc)
        if mutual and mutual["id"] != PROFILE_ID:
            platform_clause = (
                f" on {discovery_platform_label}" if discovery_platform_label else ""
            )
            msg = (
                f"Hi {target_handle}, I came across your profile through our "
                f"mutual connection {mutual['handle']}{platform_clause}. "
                f"I noticed we share a connection through {mutual['handle']} "
                f"and wanted to reach out directly to say hi — let me know "
                f"if you'd be open to connecting."
            )
        else:
            # Direct 1-hop from me — no mutual. Simpler opener.
            msg = (
                f"Hi {target_handle}, I came across your profile on "
                f"{PLATFORM_LABEL.get(acc['platform'], acc['platform'])} and "
                f"wanted to reach out directly. I'd love to connect briefly — "
                f"let me know if that works for you."
            )
        out.append({
            "platform": acc["platform"],
            "platform_label": PLATFORM_LABEL.get(acc["platform"], acc["platform"]),
            "handle": acc["handle"],
            "message": msg,
            "compose_url": compose,
            "profile_url": acc["url"],
            "via_mutual": mutual["name"] if mutual and mutual["id"] != PROFILE_ID else None,
            "via_platform": discovery_platform_label,
        })
    return out


# ---- handle-scoped persistence -------------------------------------------
#
# Both files namespace their contents under the current profile's handle
# key so distinct handles never share outreach state. Legacy flat files
# (pre-handle-scoping) are migrated lazily under the current handle on
# first read so nobody loses history.

def _current_handle_key() -> str:
    return handle_key(load_profile() or {})


def _load_all_outreach_raw() -> dict:
    if not os.path.exists(OUTREACH_STATUS_PATH):
        return {}
    try:
        with open(OUTREACH_STATUS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    if not isinstance(data, dict):
        return {}
    # New-schema entries have top-level keys containing ':' (platform:handle).
    # Old-schema files are flat person_id → status dicts; migrate them under
    # the current handle so the user's tracked history is preserved.
    if data and not any(":" in k for k in data.keys()):
        current = _current_handle_key()
        return {current: data} if current else {}
    return data


def load_outreach_status() -> dict:
    """Return the current handle's outreach map. Empty when no handle set."""
    current = _current_handle_key()
    if not current:
        return {}
    return _load_all_outreach_raw().get(current, {}) or {}


def save_outreach_status(all_status: dict) -> None:
    """Persist outreach map for the CURRENT handle.

    Other handles' data is preserved in place — this file is a multi-handle
    store, so other profiles' history isn't touched on save.
    """
    current = _current_handle_key()
    all_data = _load_all_outreach_raw()
    if current:
        all_data[current] = all_status
    with open(OUTREACH_STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2)


# ---- pending in-system messages (handle-scoped) -------------------------

def _load_all_pending_raw() -> dict:
    if not os.path.exists(PENDING_MESSAGES_PATH):
        return {}
    try:
        with open(PENDING_MESSAGES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    # Legacy flat list → wrap under current handle.
    if isinstance(data, list):
        current = _current_handle_key()
        return {current: data} if current else {}
    if not isinstance(data, dict):
        return {}
    return data


def load_pending_messages() -> list[dict]:
    current = _current_handle_key()
    if not current:
        return []
    all_data = _load_all_pending_raw()
    lst = all_data.get(current, [])
    return lst if isinstance(lst, list) else []


def save_pending_messages(messages: list[dict]) -> None:
    current = _current_handle_key()
    all_data = _load_all_pending_raw()
    if current:
        all_data[current] = messages
    with open(PENDING_MESSAGES_PATH, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2)


def _pending_for_target(target_id: str) -> list[dict]:
    return [m for m in load_pending_messages() if m.get("target_id") == target_id]


def _profile_handle() -> str:
    """Preferred handle for the user's profile, used as the From: field."""
    prof = load_profile() or {}
    tw = (prof.get("twitter") or "").strip()
    if tw:
        return tw if tw.startswith("@") else f"@{tw}"
    fc = (prof.get("farcaster") or "").strip()
    if fc:
        return fc if fc.startswith("@") else f"@{fc}"
    li = (prof.get("linkedin") or "").strip()
    if li:
        return li
    db = (prof.get("debank") or "").strip()
    if db:
        return db
    return prof.get("name", "") or "you"


def _status_for(target_id: str) -> dict:
    """Read the saved tracking state for a single target.

    Shape:
      {"visited_profile": bool, "message_sent": bool, "state": str}
    `state` is derived from the two booleans for direct UI rendering.
    """
    raw = load_outreach_status().get(target_id) or {}
    visited = bool(raw.get("visited_profile"))
    sent = bool(raw.get("message_sent"))
    if sent:
        state = "sent"
        label = "Message Sent"
    elif visited:
        state = "visited"
        label = "Profile Visited"
    else:
        state = "none"
        label = "Not Started"
    # F3 — also surface the 0-4 stage so the pipeline UI initializes.
    stage = extras.stage_from_raw(raw)
    return {
        "visited_profile": visited,
        "message_sent": sent,
        "state": state,
        "label": label,
        "stage": stage,
        "stage_label": extras.STAGE_LABELS.get(stage, ""),
        "reply_received": bool(raw.get("reply_received")),
        "meeting_scheduled": bool(raw.get("meeting_scheduled")),
        "goal_achieved": bool(raw.get("goal_achieved")),
    }


# ---- composite scoring + ranking --------------------------------------

def _composite_score(path_nodes: list[str], total_strength: float) -> dict:
    """Weighted score mixing hops, mutual platforms, and strength.

    hop_score       = 1 / hops                   (fewer hops → higher)
    mutual_score    = avg_mutuals_per_hop / 4    (more shared platforms → higher)
    strength_score  = avg_strength_per_hop / 10  (stronger ties → higher)
    score = 0.40*hop_score + 0.35*mutual_score + 0.25*strength_score
    """
    hops = max(len(path_nodes) - 1, 1)
    hop_score = 1.0 / hops
    mutuals = [
        _mutual_count(path_nodes[i], path_nodes[i + 1])
        for i in range(len(path_nodes) - 1)
    ]
    avg_mutuals = sum(mutuals) / len(mutuals) if mutuals else 0.0
    mutual_score = avg_mutuals / 4.0
    avg_strength = (total_strength / hops) if hops else 0.0
    strength_score = min(avg_strength / 10.0, 1.0)
    score = 0.40 * hop_score + 0.35 * mutual_score + 0.25 * strength_score
    return {
        "score": score,
        "components": {
            "hop_score": hop_score,
            "mutual_score": mutual_score,
            "strength_score": strength_score,
            "avg_mutuals": avg_mutuals,
            "avg_strength": avg_strength,
        },
    }


def _rank_paths(raw_paths: list[dict]) -> list[dict]:
    """Apply composite score to Yen candidates and keep the top TOP_K."""
    scored = []
    for p in raw_paths:
        s = _composite_score(p["nodes"], p["total_strength"])
        scored.append({**p, "composite": s})
    scored.sort(key=lambda p: (-p["composite"]["score"], p["cost"]))
    return scored[:TOP_K]


# ---- search index (every person, every degree) -----------------------

def _search_index(degree_map: dict[str, int]) -> list[dict]:
    """List every person in the network with their degree + all handles.

    Consumed by the client-side autocomplete. Excludes the profile
    itself since the user can't warm-intro to themselves.
    """
    out = []
    for pid in graph.id_to_name:
        if pid == PROFILE_ID:
            continue
        accs = accounts_for(pid)
        name = graph.id_to_name.get(pid, pid)
        d = degree_map.get(pid)
        # search tokens: name, id, every handle (with and without @)
        tokens = [name.lower(), pid.lower()]
        for a in accs:
            h = a["handle"].lower()
            tokens.append(h)
            if h.startswith("@"):
                tokens.append(h[1:])
        out.append({
            "id": pid,
            "name": name,
            "degree": d,
            "degree_label": degree_label(d),
            "handles": [a["handle"] for a in accs],
            "tokens": tokens,
        })
    out.sort(key=lambda r: ((r["degree"] if r["degree"] is not None else 99), r["name"].lower()))
    return out


def _resolve_target(query: str, index: list[dict]) -> str | None:
    """Resolve raw user input to a person_id or None.

    Matching priority:
      1. Exact person_id (case-insensitive)
      2. Exact handle match on any platform (with or without `@`)
      3. Exact name (case-insensitive)
      4. Substring match on name / handles — first hit wins
    """
    q = (query or "").strip().lower().lstrip("@")
    if not q:
        return None
    # 1. id match
    for pid in graph.id_to_name:
        if pid.lower() == q and pid != PROFILE_ID:
            return pid
    # 2 & 3. exact matches on tokens
    for row in index:
        if row["name"].lower() == q:
            return row["id"]
        for t in row["tokens"]:
            if t.lstrip("@") == q:
                return row["id"]
    # 4. substring match
    for row in index:
        for t in row["tokens"]:
            if q in t:
                return row["id"]
    return None


# ---- view builders -----------------------------------------------------

def _profile_view() -> dict:
    prof = load_profile() or {}
    configured = any(
        (prof.get(k) or "").strip()
        for k in ("name", "twitter", "farcaster", "linkedin", "debank")
    )
    accounts = []
    for platform, value in (
        ("twitter", prof.get("twitter")),
        ("farcaster", prof.get("farcaster")),
        ("linkedin", prof.get("linkedin")),
        ("wallet", prof.get("debank")),
    ):
        if not value:
            continue
        url = link_for(platform, value)
        if url:
            accounts.append({"platform": platform, "handle": value, "url": url})
    return {
        "name": prof.get("name", "") or "You",
        "twitter": prof.get("twitter", ""),
        "farcaster": prof.get("farcaster", ""),
        "linkedin": prof.get("linkedin", ""),
        "debank": prof.get("debank", ""),
        "accounts": accounts,
        "configured": configured,
        "has_social_handles": _profile_has_handles(prof),
    }


def _dataset_view() -> dict:
    return {
        "people": PEOPLE,
        "edges": EDGES,
        "count": len(graph.id_to_name),
    }


def _network_stats() -> dict:
    """Summary stats shown in the dashboard row above the graph.

    Recomputed on every render so edits to the dataset (profile save
    → regenerate network) are reflected without a process restart.

    `per_platform_counts` is scoped to the user's active platforms
    (platforms they filled in on their profile) — matches the BUG2
    scoping pattern used elsewhere in the app.
    """
    edge_strengths = list(graph.edge_strength.values())
    total_edges = len(edge_strengths)
    strong = sum(1 for s in edge_strengths if s >= 10)
    moderate = sum(1 for s in edge_strengths if 5 <= s < 10)
    avg_strength = (sum(edge_strengths) / total_edges) if total_edges else 0.0
    platforms_present: set[str] = {acc.platform for acc in graph.accounts}

    active = _active_platforms()
    by_platform: dict[str, set[str]] = {p: set() for p in active}
    for acc in graph.accounts:
        if acc.platform in active and (acc.handle or "").strip():
            by_platform[acc.platform].add(acc.owner_person_id)
    # Emit in a stable order matching the profile UI.
    order = ("twitter", "farcaster", "linkedin", "wallet")
    per_platform_counts = [
        {
            "platform": p,
            "label": PLATFORM_LABEL.get(p, p.title()),
            "count": len(by_platform.get(p, set())),
        }
        for p in order if p in active
    ]

    return {
        "people_count":   len(graph.id_to_name),
        "edges_count":    total_edges,
        "strong_count":   strong,
        "moderate_count": moderate,
        "platforms_count": len(platforms_present),
        "avg_strength":   round(avg_strength, 1),
        "per_platform_counts": per_platform_counts,
    }


MAX_GRAPH_NODES = 150
MAX_GRAPH_EDGES = 300
VALID_GRAPH_MODES = ("path", "first", "second", "full")


def _network_graph_data(path_ids: list[str] | None = None,
                        mode: str = "first") -> dict:
    """Compact node+edge list for the vis-network visualization.

    `mode` selects which slice of the graph to render:
      - "path"   — only nodes in the warm-intro path (fastest; falls back
                   to "first" if no path is given).
      - "first"  — me + direct 1st-degree neighbors (default for initial
                   page loads with no result).
      - "second" — me + 1st + 2nd-degree neighbors (BFS depth 2).
      - "full"   — top-degree subsample across the whole network.
    All modes cap at MAX_GRAPH_NODES nodes / MAX_GRAPH_EDGES edges.
    Path nodes are always included regardless of mode (so the user still
    sees the path after switching to "first").
    """
    if mode not in VALID_GRAPH_MODES:
        mode = "first"
    if mode == "path" and not path_ids:
        mode = "first"

    highlighted_nodes = set(path_ids or [])
    highlighted_edges: set[tuple[str, str]] = set()
    for i in range(len(path_ids or []) - 1):
        a, b = path_ids[i], path_ids[i + 1]
        highlighted_edges.add((a, b) if a <= b else (b, a))

    kept: set[str] = set()
    if mode == "path":
        kept = set(path_ids or [])
    elif mode == "full":
        deg_rank = sorted(
            graph.id_to_name.keys(),
            key=lambda n: len(graph.adjacency.get(n, ())),
            reverse=True,
        )
        kept = set(deg_rank[:MAX_GRAPH_NODES])
        if PROFILE_ID in graph.id_to_name:
            kept.add(PROFILE_ID)
        kept |= highlighted_nodes
    else:
        depth = 2 if mode == "second" else 1
        if PROFILE_ID in graph.id_to_name:
            kept.add(PROFILE_ID)
            frontier: set[str] = {PROFILE_ID}
            for _ in range(depth):
                next_frontier: set[str] = set()
                for n in frontier:
                    for nb in graph.adjacency.get(n, ()):
                        if nb not in kept:
                            next_frontier.add(nb)
                            kept.add(nb)
                frontier = next_frontier
                if len(kept) >= MAX_GRAPH_NODES:
                    break
        kept |= highlighted_nodes

    # Enforce node cap: prioritize me + path + highest-degree rest.
    if len(kept) > MAX_GRAPH_NODES:
        priority: list[str] = []
        if PROFILE_ID in kept:
            priority.append(PROFILE_ID)
        for p in path_ids or []:
            if p in kept and p not in priority:
                priority.append(p)
        priority_set = set(priority)
        remainder = sorted(
            (n for n in kept if n not in priority_set),
            key=lambda n: len(graph.adjacency.get(n, ())),
            reverse=True,
        )
        kept = priority_set | set(remainder[:MAX_GRAPH_NODES - len(priority_set)])

    degree_count: dict[str, int] = {
        pid: len(graph.adjacency.get(pid, set())) for pid in kept
    }

    nodes = []
    for pid in kept:
        name = graph.id_to_name.get(pid, pid)
        nodes.append({
            "id": pid,
            "label": name if len(name) <= 14 else name[:12] + "…",
            "title": f"{name} ({pid})",
            "degree": degree_count.get(pid, 0),
            "is_me": pid == PROFILE_ID,
            "highlight": pid in highlighted_nodes,
            "in_path": pid in highlighted_nodes,
        })

    edges = []
    for (a, b), s in graph.edge_strength.items():
        if a not in kept or b not in kept:
            continue
        bucket = strength_bucket(s)
        is_hi = (a, b) in highlighted_edges or (b, a) in highlighted_edges
        edges.append({
            "from": a, "to": b,
            "strength": int(s),
            "band": bucket["band"],
            "highlight": is_hi,
        })

    # Enforce edge cap: keep all highlighted edges + strongest remainder.
    if len(edges) > MAX_GRAPH_EDGES:
        edges.sort(key=lambda e: (0 if e["highlight"] else 1, -e["strength"]))
        edges = edges[:MAX_GRAPH_EDGES]

    return {
        "nodes": nodes,
        "edges": edges,
        "mode": mode,
        "has_path": bool(path_ids),
        "total_nodes": len(graph.id_to_name),
        "total_edges": len(graph.edge_strength),
    }


def _quick_stats_for_paths(paths: list[dict]) -> dict | None:
    """Post-path-computation summary: strongest / weakest hop across the
    best path, plus the recommended outreach platform (= platform of
    the first Option B entry on the best path, which already ranks
    per compose-url availability).
    """
    if not paths:
        return None
    best = paths[0]
    if not best["nodes"] or best["hops"] == 0:
        return None
    hops_info = []
    for i in range(len(best["nodes"]) - 1):
        a, b = best["nodes"][i], best["nodes"][i + 1]
        s = graph.strength(a["id"], b["id"])
        hops_info.append({
            "from_handle": a["handle"] if i > 0 else "You",
            "to_handle":   b["handle"],
            "strength":    int(s),
        })
    strongest = max(hops_info, key=lambda h: h["strength"])
    weakest   = min(hops_info, key=lambda h: h["strength"])
    recommended_platform = None
    recommended_label = None
    if best.get("direct_outreach"):
        first = best["direct_outreach"][0]
        recommended_platform = first["platform"]
        recommended_label    = first["platform_label"]
    return {
        "best_hops":   best["hops"],
        "strongest":   strongest,
        "weakest":     weakest,
        "recommended_platform": recommended_platform,
        "recommended_label":    recommended_label,
    }


def _render(
    target_query: str = "",
    error: str | None = None,
    result: dict | None = None,
    not_found_query: str | None = None,
):
    degree_map = compute_degrees()
    index = _search_index(degree_map)
    # Highlight the best path in the graph viz when we have a result.
    highlight_ids = None
    if result and result.get("paths"):
        highlight_ids = [n["id"] for n in result["paths"][0]["nodes"]]
    # Initial mode: "path" when a result is present (Simple default per spec),
    # otherwise "first" (me + 1st-degree only). User can switch via the toggle.
    initial_mode = "path" if highlight_ids else "first"
    graph_data = _network_graph_data(highlight_ids, mode=initial_mode)

    # ----- additive context for the remaining add-on features ----------
    outreach_store = load_outreach_status()
    outreach_stats = extras.outreach_stats(outreach_store)

    # Augment per-path result with derived metrics consumed by the
    # Path Comparison modal.
    if result and result.get("reachable") and result.get("paths"):
        for idx, p in enumerate(result["paths"]):
            hops = p.get("hop_strengths") or []
            strengths = [int(h.get("strength") or 0) for h in hops]
            # --- Success Likelihood (inverse of risk) ---
            if not strengths:
                likelihood, lband = "Unknown", "unknown"
            elif min(strengths) >= 10:
                likelihood, lband = "High", "high"
            elif min(strengths) >= 5:
                likelihood, lband = "Medium", "medium"
            else:
                likelihood, lband = "Low", "low"
            p["success_likelihood"] = likelihood
            p["success_likelihood_band"] = lband

            # --- Connection Strength bar (scale total_strength → 0..50) ---
            total = float(p.get("total_strength") or 0)
            pct = int(round(min(total / 50.0, 1.0) * 100))
            if total >= 37:   cs_band, cs_label = "very-strong", "Very Strong"
            elif total >= 25: cs_band, cs_label = "strong",      "Strong"
            elif total >= 13: cs_band, cs_label = "moderate",    "Moderate"
            else:             cs_band, cs_label = "weak",        "Weak"
            p["connection_strength_pct"]   = pct
            p["connection_strength_band"]  = cs_band
            p["connection_strength_label"] = cs_label

            # --- Interaction Quality (avg interaction_score over non-me nodes) ---
            nodes = p.get("nodes") or []
            interaction_scores = [
                int(n.get("interaction_score") or 0)
                for n in nodes if n.get("id") != PROFILE_ID
            ]
            avg_i = (sum(interaction_scores) / len(interaction_scores)) if interaction_scores else 0.0
            if avg_i >= 11:  iq_emoji, iq_label, iq_band = "🔥", "High Activity",     "high"
            elif avg_i >= 6: iq_emoji, iq_label, iq_band = "💪", "Moderate Activity", "moderate"
            else:            iq_emoji, iq_label, iq_band = "👋", "Low Activity",      "low"
            p["interaction_avg"]   = round(avg_i, 1)
            p["interaction_emoji"] = iq_emoji
            p["interaction_label"] = iq_label
            p["interaction_band"]  = iq_band

            # --- Why this path is recommended ---
            mutual = nodes[-2] if len(nodes) >= 3 else None
            target_node = nodes[-1] if nodes else None
            platform_label = (p.get("direct_outreach") or [{}])[0].get("platform_label", "")
            if idx == 0:
                if mutual and platform_label:
                    reason = (
                        f"This is the most direct route with the strongest "
                        f"interactions. {mutual['handle']} actively engages "
                        f"with both you and {target_node['handle']} on {platform_label}."
                    )
                elif mutual:
                    reason = (
                        f"This is the most direct route. {mutual['handle']} "
                        f"is your strongest mutual to {target_node['handle']}."
                    )
                else:
                    reason = "Direct reach — no intermediaries needed."
            else:
                best = result["paths"][0]
                if p["hops"] > best["hops"]:
                    reason = "This path has more steps but similar connection strength."
                elif p.get("total_strength", 0) > best.get("total_strength", 0):
                    reason = "Slightly longer, but the connections are a touch warmer."
                else:
                    reason = "A solid backup option if the best path doesn't work."
            p["why_recommended"] = reason

        result["best_time"] = extras.best_time(
            result["target"]["id"], graph,
            target_handle=result["target"].get("handle"),
            active_platforms=_active_platforms(),
        )

    profile_view = _profile_view()
    return render_template(
        "index.html",
        profile=profile_view,
        search_index=index,
        search_index_json=json.dumps(index),
        dataset=_dataset_view(),
        stats=_network_stats(),
        graph_data_json=json.dumps(graph_data),
        target_query=target_query,
        result=result,
        error=error,
        not_found_query=not_found_query,
        outreach_stats=outreach_stats,
        ai_key_present=extras.anthropic_key_present(),
        empty_state=not profile_view["has_social_handles"],
    )


# ---- routes -------------------------------------------------------------

@app.route("/", methods=["GET"])
def index():
    return _render()


DATASET_SOURCE_PATH = os.environ.get("WARM_INTRO_DATASET_SOURCE", ".dataset_source")


def _is_real_dataset() -> bool:
    """Returns True when a real (non-synthetic) graph is loaded.

    Presence of `.dataset_source` acts as the sentinel. When present,
    profile saves *augment* the graph instead of regenerating it.
    """
    return os.path.exists(DATASET_SOURCE_PATH)


def _inject_user_into_real_graph(profile: dict, *, seed_ties: int = 8) -> None:
    """Splice the profile's handle(s) into the currently-loaded real graph.

    Idempotent: removes any prior `me` row/edges/identity, then appends a
    fresh one. Attaches `me` to the top-N highest-degree nodes by
    inserting mutual-tier edges at strength 7 so pathfinding has seed
    ties to traverse.

    Never touches rows for other ids — the real dataset is preserved.
    """
    import csv

    # --- people.csv: drop old "me", append fresh -----------------------
    people_rows: list[list[str]] = []
    header: list[str] = ["id"]
    if os.path.exists(PEOPLE):
        with open(PEOPLE, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)
            if rows:
                header = rows[0]
                people_rows = [r for r in rows[1:] if (r and r[0] != PROFILE_ID)]
    people_rows.append([PROFILE_ID] + [""] * (len(header) - 1))
    with open(PEOPLE, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(people_rows)

    # --- identities.csv: drop old "me" row, append fresh ---------------
    id_rows: list[list[str]] = []
    id_header: list[str] = []
    if os.path.exists(IDENTITIES):
        with open(IDENTITIES, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)
            if rows:
                id_header = rows[0]
                id_rows = [r for r in rows[1:] if (r and r[0] != PROFILE_ID)]
    # Build a "me" row matching the dataset's column shape.
    col_index = {c: i for i, c in enumerate(id_header)} if id_header else {}
    me_row = [""] * max(len(id_header), 1)
    if id_header and id_header[0] == "person_id":
        me_row[0] = PROFILE_ID
        for src, col in (
            ("twitter",   "twitter"),
            ("farcaster", "farcaster"),
            ("linkedin",  "linkedin"),
            ("debank",    "debank"),
        ):
            val = (profile.get(src) or "").strip()
            if val and col in col_index:
                me_row[col_index[col]] = val if val.startswith("@") or col in ("linkedin", "debank") else f"@{val}"
        if "interaction_score" in col_index:
            me_row[col_index["interaction_score"]] = "0"
    id_rows.append(me_row)
    with open(IDENTITIES, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(id_header or ["person_id"])
        w.writerows(id_rows)

    # --- edges.csv: drop old "me" edges, append seed ties --------------
    edge_header: list[str] = ["from", "to", "strength", "tier"]
    edge_rows: list[list[str]] = []
    degree: dict[str, int] = {}
    if os.path.exists(EDGES):
        with open(EDGES, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)
            if rows:
                edge_header = rows[0]
                for r in rows[1:]:
                    if not r or len(r) < 2:
                        continue
                    a, b = r[0], r[1]
                    if a == PROFILE_ID or b == PROFILE_ID:
                        continue
                    edge_rows.append(r)
                    degree[a] = degree.get(a, 0) + 1
                    degree[b] = degree.get(b, 0) + 1
    # Pick the top-K highest-degree real nodes as seed ties.
    seeds = sorted(degree, key=lambda n: degree[n], reverse=True)[:seed_ties]
    # Edge column order: from, to, strength, tier.
    for s in seeds:
        row = [""] * len(edge_header)
        try:
            row[edge_header.index("from")]     = PROFILE_ID
            row[edge_header.index("to")]       = s
            if "strength" in edge_header:
                row[edge_header.index("strength")] = "7"
            if "tier" in edge_header:
                row[edge_header.index("tier")] = "mutual"
        except ValueError:
            # Fallback for unexpected header shape.
            row = [PROFILE_ID, s, "7", "mutual"]
        edge_rows.append(row)
    with open(EDGES, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(edge_header)
        w.writerows(edge_rows)


@app.route("/profile", methods=["POST"])
def update_profile():
    prof = {
        "name":      (request.form.get("name")      or "").strip(),
        "twitter":   (request.form.get("twitter")   or "").strip(),
        "farcaster": (request.form.get("farcaster") or "").strip(),
        "linkedin":  (request.form.get("linkedin")  or "").strip(),
        "debank":    (request.form.get("debank")    or "").strip(),
    }
    save_profile(prof)
    if _is_real_dataset():
        # Real graph loaded — augment it, don't clobber. The user's handle
        # is spliced in with seed ties to high-degree nodes so pathfinding
        # has something to traverse.
        if _profile_has_handles(prof):
            _inject_user_into_real_graph(prof)
        else:
            # No handles → remove "me" from the real graph (same idempotent
            # helper, minus the inject step). Cheapest path: re-inject with
            # an empty profile so only the person row exists, no edges.
            _inject_user_into_real_graph(prof, seed_ties=0)
    else:
        # Synthetic-data path — preserves original behavior exactly.
        _clear_dataset()
        if _profile_has_handles(prof):
            seed_for_profile(prof)
    rebuild_graph()
    return redirect(url_for("index"))


@app.route("/search", methods=["POST"])
def search():
    # BUG1 — refuse search when profile has no handles.
    if not _profile_has_handles(load_profile()):
        return _render()
    query = (request.form.get("target") or "").strip()
    if not query:
        return _render(error="Type a target person by name, handle, or wallet.")
    if PROFILE_ID not in graph.id_to_name:
        return _render(
            target_query=query,
            error="Your profile isn't in the graph yet. Save your profile above to seed the network.",
        )

    degree_map = compute_degrees()
    index = _search_index(degree_map)
    target_id = _resolve_target(query, index)
    if target_id is None:
        return _render(
            target_query=query,
            not_found_query=query,
        )

    # Pull a wider candidate pool, score, rerank, take top 3.
    raw = yen_k_shortest_paths(graph, PROFILE_ID, target_id, CANDIDATE_POOL)
    if not raw:
        return _render(
            target_query=query,
            result={
                "reachable": False,
                "target": node_view(target_id, degree_map=degree_map),
                "target_query": query,
                "paths": [],
                "explanation": (
                    f"No path exists from you to "
                    f"{graph.id_to_name.get(target_id, target_id)} in the "
                    f"current network."
                ),
            },
        )
    candidates = [
        {
            "nodes": path,
            "cost": cost,
            "hops": len(path) - 1,
            "total_strength": sum(
                graph.strength(path[j], path[j + 1]) for j in range(len(path) - 1)
            ),
        }
        for cost, path in raw
    ]
    ranked = _rank_paths(candidates)

    paths_view = []
    for rank, p in enumerate(ranked, start=1):
        medal, label = MEDALS.get(rank, ("", ""))
        nodes = [node_view(nid, degree_map=degree_map) for nid in p["nodes"]]
        target_node = nodes[-1]
        direct_outreach = _build_direct_outreach(nodes, target_node)
        mutual_panels = [
            {
                "from": nodes[i],
                "to": nodes[i + 1],
                "platforms": mutual_status(nodes[i]["id"], nodes[i + 1]["id"]),
            }
            for i in range(len(nodes) - 1)
        ]
        # Per-hop strength bucket for the colour-coded arrow + pill.
        hop_strengths = [
            {
                "strength": graph.strength(nodes[i]["id"], nodes[i + 1]["id"]),
                "bucket": strength_bucket(graph.strength(nodes[i]["id"], nodes[i + 1]["id"])),
            }
            for i in range(len(nodes) - 1)
        ]
        paths_view.append({
            "rank": rank,
            "medal": medal,
            "label": label,
            "nodes": nodes,
            "hops": p["hops"],
            "total_strength": p["total_strength"],
            "cost": p["cost"],
            "target": target_node,
            "score": p["composite"]["score"],
            "score_components": p["composite"]["components"],
            "direct_outreach": direct_outreach,
            "mutual_panels": mutual_panels,
            "hop_strengths": hop_strengths,
        })

    target_node = node_view(target_id, degree_map=degree_map)
    status = _status_for(target_id)
    target_handle = target_node["handle"] or target_node["name"]
    from_handle = _profile_handle()
    pending = _pending_for_target(target_id)
    # Default pre-written in-system composer message. Uses the first
    # path's mutual if available so the opener is warm by default.
    default_msg = (
        f"Hi {target_handle}, I'd love to connect! I came across your "
        f"profile through our shared network and wanted to reach out. "
        f"Looking forward to hearing from you."
    )
    if paths_view:
        first = paths_view[0]
        if len(first["nodes"]) >= 3:
            mutual = first["nodes"][-2]
            default_msg = (
                f"Hi {target_handle}, I came across your profile through "
                f"our mutual connection {mutual['handle']}. I'd love to "
                f"connect and chat briefly — let me know if you're open "
                f"to it."
            )
    result_view = {
        "reachable": True,
        "target": target_node,
        "target_query": query,
        "target_status": status,
        "paths": paths_view,
        "from_handle": from_handle,
        "to_handle": target_handle,
        "composer_message": default_msg,
        "pending_messages": pending,
        "quick_stats": _quick_stats_for_paths(paths_view),
        "explanation": (
            f"Searched {len(candidates)} candidate path(s); kept the top "
            f"{len(paths_view)} by composite score "
            f"(40% hops · 35% mutuals · 25% strength)."
        ),
    }
    return _render(target_query=query, result=result_view)


@app.route("/messages/send", methods=["POST"])
def messages_send():
    """Queue an in-system warm-intro message.

    Stored with status="pending" in `pending_messages.json`. The UI
    shows this as "Message queued! Login to activate sending." — the
    actual delivery is stubbed until social logins exist.
    """
    target_id = (request.form.get("target_id") or "").strip()
    message = (request.form.get("message") or "").strip()
    from_handle = (request.form.get("from_handle") or "").strip()
    to_handle = (request.form.get("to_handle") or "").strip()
    platform = (request.form.get("platform") or "in-system").strip()
    if not target_id or not message:
        return jsonify({"ok": False, "error": "target_id and message required"}), 400
    entry = {
        "id": uuid.uuid4().hex[:12],
        "target_id": target_id,
        "from_handle": from_handle,
        "to_handle": to_handle,
        "platform": platform,
        "message": message,
        "status": "pending",
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    messages = load_pending_messages()
    messages.append(entry)
    save_pending_messages(messages)
    return jsonify({
        "ok": True,
        "message": entry,
        "pending": _pending_for_target(target_id),
    })


@app.route("/outreach/status", methods=["POST"])
def outreach_status():
    """Persist the outreach tracking state for a single target.

    Accepts form fields `target_id` (required), and any subset of
    `visited_profile` / `message_sent` as "true"/"false". Returns the
    new state as JSON so the client can update its UI inline.
    """
    target_id = (request.form.get("target_id") or "").strip()
    if not target_id:
        return jsonify({"ok": False, "error": "target_id required"}), 400
    all_status = load_outreach_status()
    cur = all_status.get(target_id) or {}
    if "visited_profile" in request.form:
        cur["visited_profile"] = request.form.get("visited_profile") == "true"
    if "message_sent" in request.form:
        cur["message_sent"] = request.form.get("message_sent") == "true"
    # Sending implies visited.
    if cur.get("message_sent"):
        cur["visited_profile"] = True
    all_status[target_id] = cur
    save_outreach_status(all_status)
    return jsonify({"ok": True, "status": _status_for(target_id)})


# =========================================================================
# F1 / F3 / F5 / F8 routes — additive only (no existing route changed)
# =========================================================================

@app.route("/ai/generate-intro", methods=["POST"])
def ai_generate_intro():
    target_id = (request.form.get("target_id") or "").strip()
    mutual_id = (request.form.get("mutual_id") or "").strip()
    platform  = (request.form.get("platform")  or "").strip().lower()
    style     = (request.form.get("style")     or "warm").strip()
    if not target_id or target_id not in graph.id_to_name:
        return jsonify({"ok": False, "error": "unknown target"}), 400
    if not extras.anthropic_key_present():
        return jsonify({
            "ok": False,
            "error": "Set ANTHROPIC_API_KEY to enable AI generation.",
        }), 400

    profile = load_profile() or {}
    sender_name   = profile.get("name", "") or "You"
    sender_handle = _profile_handle()

    target_name   = graph.id_to_name.get(target_id, target_id)
    target_handle = _preferred_handle(target_id)

    mutual_name = mutual_handle = None
    if mutual_id and mutual_id in graph.id_to_name and mutual_id != PROFILE_ID:
        mutual_name = graph.id_to_name.get(mutual_id, mutual_id)
        mutual_handle = _preferred_handle(mutual_id)

    platform_label = PLATFORM_LABEL.get(platform, platform.title() or "direct")

    # Summarise target's interaction volume for context.
    accs = accounts_for(target_id)
    chips = _interaction_summary(accs)
    summary = ", ".join(
        f"{c['count']} {c['label'].lower()}" for c in chips[:4]
    ) or f"style:{style}"

    try:
        message = extras.generate_intro_message(
            sender_name=sender_name,
            sender_handle=sender_handle,
            mutual_name=mutual_name,
            mutual_handle=mutual_handle,
            target_name=target_name,
            target_handle=target_handle,
            platform_label=platform_label,
            interaction_summary=summary,
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 502
    return jsonify({"ok": True, "message": message})


@app.route("/outreach/stage", methods=["POST"])
def outreach_stage():
    """F3 — advance/reset a target's stage 0..4."""
    target_id = (request.form.get("target_id") or "").strip()
    try:
        stage = int(request.form.get("stage") or "0")
    except ValueError:
        return jsonify({"ok": False, "error": "invalid stage"}), 400
    if not target_id:
        return jsonify({"ok": False, "error": "target_id required"}), 400
    if target_id not in graph.id_to_name:
        return jsonify({"ok": False, "error": "unknown target_id"}), 400
    all_status = load_outreach_status()
    all_status[target_id] = extras.apply_stage(all_status.get(target_id), stage)
    save_outreach_status(all_status)
    return jsonify({
        "ok": True,
        "stage": all_status[target_id]["stage"],
        "stage_label": extras.STAGE_LABELS.get(all_status[target_id]["stage"], ""),
        "status": _status_for(target_id),
        "outreach_stats": extras.outreach_stats(all_status),
    })


@app.route("/guide", methods=["GET"])
def guide_view():
    """FIX3 — beginner-friendly How-to-Use guide."""
    return render_template(
        "guide.html",
        profile=_profile_view(),
        dataset=_dataset_view(),
    )


@app.route("/graph/data", methods=["GET"])
def graph_data_route():
    """On-demand graph slice for the mode-toggle UI.

    Query params:
      - mode: one of VALID_GRAPH_MODES. Defaults to "first".
      - target: optional target person id; when given, we recompute the
        warm-intro path so the slice highlights it correctly.
    """
    mode = (request.args.get("mode") or "first").strip()
    target_id = (request.args.get("target") or "").strip() or None
    path_ids: list[str] | None = None
    if target_id and target_id in graph.id_to_name and PROFILE_ID in graph.id_to_name:
        raw = yen_k_shortest_paths(graph, PROFILE_ID, target_id, CANDIDATE_POOL)
        candidates = [
            {
                "nodes": p,
                "cost": c,
                "hops": len(p) - 1,
                "total_strength": sum(
                    graph.strength(p[j], p[j + 1]) for j in range(len(p) - 1)
                ),
            }
            for c, p in raw
        ]
        ranked = _rank_paths(candidates) if candidates else []
        if ranked:
            path_ids = ranked[0]["nodes"]
    return jsonify(_network_graph_data(path_ids, mode=mode))


@app.route("/tracker", methods=["GET"])
def tracker_view():
    """F10 — Outreach Tracker dashboard."""
    outreach_store = load_outreach_status()
    pending_store = load_pending_messages()
    rows_raw = extras.tracker_rows(graph, outreach_store, pending_store)

    degree_map = compute_degrees()
    def _enrich_row(r: dict) -> dict:
        accs = accounts_for(r["target_id"])
        for a in accs:
            a["platform_label"] = PLATFORM_LABEL.get(a["platform"], a["platform"])
        return {
            **r,
            "accounts": accs,
            "handle": _preferred_handle(r["target_id"]),
            "degree": degree_map.get(r["target_id"]),
        }
    rows = [_enrich_row(r) for r in rows_raw]
    rows.sort(key=lambda r: (r["days_since"] if r["days_since"] >= 0 else 10**6), reverse=False)

    stats = extras.tracker_stats(outreach_store, pending_store)

    recon_raw = extras.reconnect_suggestions(
        graph, PROFILE_ID, outreach_store, pending_store,
    )
    def _enrich_recon(r: dict) -> dict:
        accs = accounts_for(r["id"])
        for a in accs:
            a["platform_label"] = PLATFORM_LABEL.get(a["platform"], a["platform"])
        return {**r, "accounts": accs, "handle": _preferred_handle(r["id"])}
    reconnect = [_enrich_recon(r) for r in recon_raw]

    return render_template(
        "tracker.html",
        profile=_profile_view(),
        dataset=_dataset_view(),
        rows=rows,
        stats=stats,
        reconnect=reconnect,
        stage_labels=extras.STAGE_LABELS,
        ai_key_present=extras.anthropic_key_present(),
        has_profile=PROFILE_ID in graph.id_to_name,
    )


@app.route("/tracker/detail/<target_id>", methods=["GET"])
def tracker_detail(target_id: str):
    """Return JSON detail for the tracker modal: node, warm-intro path,
    full message history, stage timeline, mutual connections."""
    if target_id not in graph.id_to_name:
        return jsonify({"ok": False, "error": "unknown target_id"}), 404

    degree_map = compute_degrees()
    node = node_view(target_id, degree_map=degree_map)

    path_view: dict | None = None
    mutuals: list[dict] = []
    if PROFILE_ID in graph.id_to_name:
        raw = yen_k_shortest_paths(graph, PROFILE_ID, target_id, CANDIDATE_POOL)
        candidates = [
            {
                "nodes": path,
                "cost": cost,
                "hops": len(path) - 1,
                "total_strength": sum(
                    graph.strength(path[j], path[j + 1]) for j in range(len(path) - 1)
                ),
            }
            for cost, path in raw
        ]
        ranked = _rank_paths(candidates) if candidates else []
        if ranked:
            best = ranked[0]
            nodes = [
                node_view(n, degree_map=degree_map) for n in best["nodes"]
            ]
            path_view = {
                "hops": best["hops"],
                "total_strength": best["total_strength"],
                "nodes": nodes,
            }
            mutuals = nodes[1:-1] if len(nodes) >= 3 else []

    messages = sorted(
        _pending_for_target(target_id),
        key=lambda m: m.get("created_at") or "",
    )
    outreach = load_outreach_status().get(target_id) or {}
    timeline = list(outreach.get("stage_history") or [])
    # Fallback: if no stage_history but message_sent, synthesize one entry.
    if not timeline and messages:
        timeline = [{
            "stage": int(outreach.get("stage") or 1),
            "at": (messages[0].get("created_at") or "")[:10],
        }]

    return jsonify({
        "ok": True,
        "node": node,
        "path": path_view,
        "messages": messages,
        "timeline": timeline,
        "mutuals": mutuals,
        "status": outreach,
    })


@app.route("/tracker/follow-up", methods=["POST"])
def tracker_followup():
    """Generate a follow-up message (template; optional AI rewrite)."""
    target_id = (request.form.get("target_id") or "").strip()
    try:
        tier = int(request.form.get("tier") or "1")
    except ValueError:
        tier = 1
    tier = max(1, min(3, tier))
    use_ai = (request.form.get("use_ai") or "").strip().lower() in ("1", "true", "yes")

    if not target_id or target_id not in graph.id_to_name:
        return jsonify({"ok": False, "error": "unknown target_id"}), 400

    target_handle = _preferred_handle(target_id)
    mutual_handle = "@your mutual"
    if PROFILE_ID in graph.id_to_name:
        raw = yen_k_shortest_paths(graph, PROFILE_ID, target_id, CANDIDATE_POOL)
        candidates = [
            {
                "nodes": path,
                "cost": cost,
                "hops": len(path) - 1,
                "total_strength": sum(
                    graph.strength(path[j], path[j + 1]) for j in range(len(path) - 1)
                ),
            }
            for cost, path in raw
        ]
        ranked = _rank_paths(candidates) if candidates else []
        if ranked and len(ranked[0]["nodes"]) >= 3:
            mutual_handle = _preferred_handle(ranked[0]["nodes"][1])

    tpl = extras.followup_template(tier, target_handle, mutual_handle)
    message = tpl["message"]

    if use_ai and extras.anthropic_key_present():
        try:
            system = ("You rewrite warm-intro follow-up messages to sound natural, "
                      "concise, and human. Keep it under 60 words. No emojis unless already present.")
            user = (f"Rewrite this follow-up (tier {tier} — {tpl['label']}):\n\n{message}\n\n"
                    f"Target handle: {target_handle}. Mutual: {mutual_handle}. "
                    "Return only the rewritten message.")
            message = extras.anthropic_complete(system, user, max_tokens=220).strip()
        except Exception:
            pass

    return jsonify({"ok": True, "message": message, "tier": tier, "label": tpl["label"]})


@app.route("/tracker/reconnect-message", methods=["POST"])
def tracker_reconnect_message():
    """Generate a short warm-reconnect message for a dormant contact."""
    target_id = (request.form.get("target_id") or "").strip()
    if not target_id or target_id not in graph.id_to_name:
        return jsonify({"ok": False, "error": "unknown target_id"}), 400
    handle = _preferred_handle(target_id)
    accs = accounts_for(target_id)
    platform_label = PLATFORM_LABEL.get(accs[0]["platform"], "there") if accs else "there"
    message = (
        f"Hey {handle}, it's been a while! Saw you're still active on "
        f"{platform_label} — would love to catch up when you have a sec."
    )
    return jsonify({"ok": True, "message": message})


@app.route("/revive", methods=["GET"])
def revive_view():
    """F9 — Dormant Bridge Surfacer.

    Ranks 1st-degree contacts by how much 2nd-degree reach they unlock
    weighted by how long since you last talked. Encourages maintenance
    of bridge ties rather than only searching when you have a target.
    """
    data = extras.dormant_bridges(graph, PROFILE_ID)
    def _enrich(row: dict) -> dict:
        accs = accounts_for(row["id"])
        for a in accs:
            a["platform_label"] = PLATFORM_LABEL.get(a["platform"], a["platform"])
        return {**row, "accounts": accs, "handle": _preferred_handle(row["id"])}
    bridges = [_enrich(r) for r in data["bridges"]]
    never = [_enrich(r) for r in data["never_logged"]]
    summary = {
        "first_degree_count": data["first_degree_count"],
        "total_unlocks": data["total_unlocks"],
        "max_months": data["max_months"],
        "has_profile": PROFILE_ID in graph.id_to_name,
    }
    return render_template(
        "revive.html",
        profile=_profile_view(),
        dataset=_dataset_view(),
        bridges=bridges,
        never_logged=never,
        summary=summary,
    )


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5000)
