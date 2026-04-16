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

from flask import Flask, jsonify, redirect, render_template, request, url_for

from seed_profile_network import (
    PROFILE_ID,
    default_profile,
    seed_for_profile,
)
from warm_intro import build_graph, find_ranked_paths, yen_k_shortest_paths

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

# UI icons + labels for the per-platform interaction counts on person
# cards. Keys match attribute keys stored on SocialAccount.attributes.
# `platform` column maps these to the platform the icons live under
# (used by the details panel for grouping).
INTERACTION_ICONS: list[dict] = [
    {"key": "twitter_likes",           "platform": "twitter",   "icon": "👍", "label": "Likes"},
    {"key": "twitter_comments",        "platform": "twitter",   "icon": "💬", "label": "Comments"},
    {"key": "twitter_reposts",         "platform": "twitter",   "icon": "🔁", "label": "Reposts"},
    {"key": "farcaster_recasts",       "platform": "farcaster", "icon": "🔄", "label": "Recasts"},
    {"key": "farcaster_replies",       "platform": "farcaster", "icon": "💬", "label": "Replies"},
    {"key": "linkedin_recommendations","platform": "linkedin",  "icon": "⭐", "label": "Recommendations"},
    {"key": "linkedin_endorsements",   "platform": "linkedin",  "icon": "🏆", "label": "Endorsements"},
    {"key": "debank_transactions",     "platform": "wallet",    "icon": "💸", "label": "On-chain txs"},
]


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


def _ensure_dataset(profile: dict) -> None:
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
    for acc in graph.accounts:
        if acc.owner_person_id != node_id:
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
    """For each of the four platforms, report whether both endpoints have
    a handle on that platform. The synthetic dataset gives every
    non-profile person all four platforms, so the profile's platform
    coverage is the thing that shapes this panel in practice."""
    a = _platforms_of(a_id)
    b = _platforms_of(b_id)
    return [
        {
            "platform": p,
            "platform_label": PLATFORM_LABEL[p],
            "a_has": p in a,
            "b_has": p in b,
            "mutual": (p in a) and (p in b),
        }
        for p in PLATFORMS
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
    shared = [
        p for p in PLATFORMS
        if p in _platforms_of(mutual_id) and p in _platforms_of(target_id)
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


# ---- outreach status persistence ----------------------------------------

def load_outreach_status() -> dict:
    if not os.path.exists(OUTREACH_STATUS_PATH):
        return {}
    try:
        with open(OUTREACH_STATUS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def save_outreach_status(all_status: dict) -> None:
    with open(OUTREACH_STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(all_status, f, indent=2)


# ---- pending in-system messages ----------------------------------------

def load_pending_messages() -> list[dict]:
    if not os.path.exists(PENDING_MESSAGES_PATH):
        return []
    try:
        with open(PENDING_MESSAGES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def save_pending_messages(messages: list[dict]) -> None:
    with open(PENDING_MESSAGES_PATH, "w", encoding="utf-8") as f:
        json.dump(messages, f, indent=2)


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
    return {
        "visited_profile": visited,
        "message_sent": sent,
        "state": state,
        "label": label,
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
    """
    edge_strengths = list(graph.edge_strength.values())
    total_edges = len(edge_strengths)
    strong = sum(1 for s in edge_strengths if s >= 10)
    moderate = sum(1 for s in edge_strengths if 5 <= s < 10)
    avg_strength = (sum(edge_strengths) / total_edges) if total_edges else 0.0
    platforms_present: set[str] = {acc.platform for acc in graph.accounts}
    return {
        "people_count":   len(graph.id_to_name),
        "edges_count":    total_edges,
        "strong_count":   strong,
        "moderate_count": moderate,
        "platforms_count": len(platforms_present),
        "avg_strength":   round(avg_strength, 1),
    }


def _network_graph_data(path_ids: list[str] | None = None) -> dict:
    """Compact node+edge list for the vis-network visualization.

    When `path_ids` is given (ordered list of node ids from the best
    path) those nodes+edges get a `highlight: true` flag so the JS can
    pop them visually.
    """
    highlighted_nodes = set(path_ids or [])
    highlighted_edges: set[tuple[str, str]] = set()
    for i in range(len(path_ids or []) - 1):
        a, b = path_ids[i], path_ids[i + 1]
        highlighted_edges.add((a, b) if a <= b else (b, a))

    # Node size driven by degree (number of adjacent nodes).
    degree_count: dict[str, int] = {
        pid: len(graph.adjacency.get(pid, set())) for pid in graph.id_to_name
    }

    nodes = []
    for pid, name in graph.id_to_name.items():
        nodes.append({
            "id": pid,
            "label": name if len(name) <= 14 else name[:12] + "…",
            "title": f"{name} ({pid})",
            "degree": degree_count.get(pid, 0),
            "is_me": pid == PROFILE_ID,
            "highlight": pid in highlighted_nodes,
        })

    edges = []
    for (a, b), s in graph.edge_strength.items():
        bucket = strength_bucket(s)
        edges.append({
            "from": a, "to": b,
            "strength": int(s),
            "band": bucket["band"],
            "highlight": (a, b) in highlighted_edges or (b, a) in highlighted_edges,
        })
    return {"nodes": nodes, "edges": edges}


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
    graph_data = _network_graph_data(highlight_ids)
    return render_template(
        "index.html",
        profile=_profile_view(),
        search_index=index,
        search_index_json=json.dumps(index),
        dataset=_dataset_view(),
        stats=_network_stats(),
        graph_data_json=json.dumps(graph_data),
        target_query=target_query,
        result=result,
        error=error,
        not_found_query=not_found_query,
    )


# ---- routes -------------------------------------------------------------

@app.route("/", methods=["GET"])
def index():
    return _render()


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
    seed_for_profile(prof)
    rebuild_graph()
    return redirect(url_for("index"))


@app.route("/search", methods=["POST"])
def search():
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


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5000)
