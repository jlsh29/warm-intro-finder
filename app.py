"""Flask UI for the warm-intro path finder.

Run:
    python app.py
Then open http://127.0.0.1:5000/
"""

from __future__ import annotations

import os

from flask import Flask, render_template, request

from warm_intro import build_graph, find_warm_intro

PEOPLE = os.environ.get("WARM_INTRO_PEOPLE", "people.csv")
EDGES = os.environ.get("WARM_INTRO_EDGES", "edges.csv")
IDENTITIES = os.environ.get("WARM_INTRO_IDENTITIES")  # optional

app = Flask(__name__)
graph = build_graph(PEOPLE, EDGES, identities_path=IDENTITIES)


def link_for(platform: str, handle: str) -> str | None:
    """Return the canonical profile URL for a (platform, handle) pair.

    Returns None for platforms we don't know how to link, so the
    template can drop them silently rather than rendering a dead
    badge. `handle` is taken verbatim from identities.csv except for
    a stripped leading `@` on platforms that don't want it.
    """
    h = (handle or "").strip()
    if not h:
        return None
    p = (platform or "").strip().lower()
    if p == "twitter":
        return f"https://twitter.com/{h.lstrip('@')}"
    if p == "linkedin":
        # Accept either a bare slug ("alice-anderson") or a full URL
        # already (paste-and-go). If it's already a URL, use as-is.
        if h.startswith("http"):
            return h
        return f"https://linkedin.com/in/{h.lstrip('@')}"
    if p == "farcaster":
        return f"https://warpcast.com/{h.lstrip('@')}"
    if p == "wallet":
        return f"https://debank.com/profile/{h}"
    return None


def accounts_for(node_id: str) -> list[dict]:
    """Return the linkable social accounts owned by `node_id`.

    Each dict carries `dm`: `"yes"` | `"no"` | `None` (None when the
    identities.csv has no `dm` column for this row). The template
    renders a DM badge only when the value is explicitly yes or no.
    """
    out: list[dict] = []
    for acc in graph.accounts:
        if acc.owner_person_id != node_id:
            continue
        url = link_for(acc.platform, acc.handle)
        if not url:
            continue  # unknown platform — drop silently
        dm_raw = (acc.attributes.get("dm") or "").strip().lower()
        dm: str | None = None
        if dm_raw in ("yes", "y", "true", "1"):
            dm = "yes"
        elif dm_raw in ("no", "n", "false", "0"):
            dm = "no"
        out.append(
            {"platform": acc.platform, "handle": acc.handle, "url": url, "dm": dm}
        )
    return out


def node_view(node_id: str) -> dict:
    return {
        "id": node_id,
        "name": graph.id_to_name.get(node_id, node_id),
        "accounts": accounts_for(node_id),
    }


def path_view(path: list[str]) -> list[dict]:
    return [node_view(p) for p in path]


@app.route("/", methods=["GET"])
def index():
    # Provide dropdown options sorted by name
    # Dropdown label shows `name (id)` when those differ, else just id.
    # Post-username-transform most persons have name==id, so the label
    # collapses to just the username.
    people_options = sorted(
        (
            {
                "id": pid,
                "label": (
                    pid
                    if graph.id_to_name.get(pid, pid) == pid
                    else f"{graph.id_to_name[pid]} ({pid})"
                ),
            }
            for pid in graph.id_to_name
        ),
        key=lambda p: p["label"].lower(),
    )
    return render_template(
        "index.html",
        people=people_options,
        dataset={"people": PEOPLE, "edges": EDGES, "count": len(graph.id_to_name)},
        form={},
        result=None,
        error=None,
    )


@app.route("/search", methods=["POST"])
def search():
    entries_raw = request.form.getlist("entries") or []
    entries_free = (request.form.get("entries_free") or "").strip()
    if entries_free:
        entries_raw.extend(
            e.strip() for e in entries_free.split(",") if e.strip()
        )
    target = (request.form.get("target") or "").strip()
    try:
        top_k = int(request.form.get("top_k") or 3)
    except ValueError:
        top_k = 3
    # Dropdown label shows `name (id)` when those differ, else just id.
    # Post-username-transform most persons have name==id, so the label
    # collapses to just the username.
    people_options = sorted(
        (
            {
                "id": pid,
                "label": (
                    pid
                    if graph.id_to_name.get(pid, pid) == pid
                    else f"{graph.id_to_name[pid]} ({pid})"
                ),
            }
            for pid in graph.id_to_name
        ),
        key=lambda p: p["label"].lower(),
    )
    form_state = {
        "entries": entries_raw,
        "entries_free": entries_free,
        "target": target,
        "top_k": top_k,
    }

    if not entries_raw:
        return render_template(
            "index.html",
            people=people_options,
            dataset={
                "people": PEOPLE, "edges": EDGES, "count": len(graph.id_to_name)
            },
            form=form_state,
            result=None,
            error="Pick or type at least one entry point.",
        )
    if not target:
        return render_template(
            "index.html",
            people=people_options,
            dataset={
                "people": PEOPLE, "edges": EDGES, "count": len(graph.id_to_name)
            },
            form=form_state,
            result=None,
            error="Pick or type a target person.",
        )

    try:
        result = find_warm_intro(graph, entries_raw, target, top_k)
    except ValueError as exc:
        return render_template(
            "index.html",
            people=people_options,
            dataset={
                "people": PEOPLE, "edges": EDGES, "count": len(graph.id_to_name)
            },
            form=form_state,
            result=None,
            error=str(exc),
        )

    view = {
        "reachable": result["best"] is not None,
        "hops": result["hops"],
        "explanation": result["explanation"],
        "best": path_view(result["best"]) if result["best"] else None,
        "alternatives": [path_view(p) for p in result["alternatives"]],
        "entry_used": node_view(result["entry_used"]) if result["entry_used"] else None,
    }
    return render_template(
        "index.html",
        people=people_options,
        dataset={"people": PEOPLE, "edges": EDGES, "count": len(graph.id_to_name)},
        form=form_state,
        result=view,
        error=None,
    )


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5000)
