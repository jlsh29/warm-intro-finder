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

app = Flask(__name__)
graph = build_graph(PEOPLE, EDGES)


def node_view(node_id: str) -> dict:
    meta = graph.id_to_meta.get(node_id, {})
    company = meta.get("company", "")
    team = meta.get("team", "")
    if company and team.startswith(f"{company} / "):
        team = team[len(company) + 3:]
    return {
        "id": node_id,
        "name": graph.id_to_name.get(node_id, node_id),
        "company": company,
        "team": team,
        "role": meta.get("role", ""),
    }


def path_view(path: list[str]) -> list[dict]:
    return [node_view(p) for p in path]


def diagram_data(result: dict, entries: list[str]) -> dict:
    """Build vis-network nodes/edges from a path-finding result.

    Best path edges are styled prominently; alternative-path edges
    are dimmed/dashed. Each edge carries a hover tooltip with the
    full strength + tier + reason explanation.
    """
    if not result.get("best"):
        return {"nodes": [], "edges": []}

    paths = [result["best"]] + list(result.get("alternatives") or [])
    entry_set = set(entries)
    target_id = result["best"][-1]

    # Collect all unique node ids across paths
    node_ids: list[str] = []
    seen: set[str] = set()
    for p in paths:
        for nid in p:
            if nid not in seen:
                seen.add(nid)
                node_ids.append(nid)

    def role(nid: str) -> str:
        if nid == target_id:
            return "target"
        if nid in entry_set or any(p[0] == nid for p in paths):
            return "entry"
        return "intermediate"

    nodes = []
    for nid in node_ids:
        meta = graph.id_to_meta.get(nid, {})
        company = meta.get("company", "")
        team = meta.get("team", "")
        if company and team.startswith(f"{company} / "):
            team = team[len(company) + 3:]
        sub = " / ".join(s for s in (company, team, meta.get("role")) if s)
        nodes.append(
            {
                "id": nid,
                "label": f"{graph.id_to_name.get(nid, nid)}\n{nid}",
                "title": sub or graph.id_to_name.get(nid, nid),
                "group": role(nid),
            }
        )

    edges = []
    seen_edges: set[tuple[str, str, int]] = set()
    for path_idx, p in enumerate(paths):
        is_best = path_idx == 0
        for i in range(1, len(p)):
            a, b = p[i - 1], p[i]
            key = (a, b, path_idx)
            if key in seen_edges:
                continue
            seen_edges.add(key)
            s = graph.strength(a, b)
            tier = graph.tier(a, b)
            reason = graph.reason(a, b)
            label_str = (
                f"{int(s) if float(s).is_integer() else round(s, 1)}|{tier}"
            )
            edges.append(
                {
                    "from": a,
                    "to": b,
                    "label": label_str,
                    "title": (
                        f"strength: {s}\\ntier: {tier}\\nreason: {reason}"
                        f"\\npath: {'BEST' if is_best else f'ALT {path_idx}'}"
                    ),
                    "best": is_best,
                    "pathIndex": path_idx,
                }
            )

    return {"nodes": nodes, "edges": edges}


@app.route("/", methods=["GET"])
def index():
    # Provide dropdown options sorted by name
    people_options = sorted(
        (
            {"id": pid, "label": f"{graph.id_to_name.get(pid, pid)} ({pid})"}
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
    explain = bool(request.form.get("explain"))

    people_options = sorted(
        (
            {"id": pid, "label": f"{graph.id_to_name.get(pid, pid)} ({pid})"}
            for pid in graph.id_to_name
        ),
        key=lambda p: p["label"].lower(),
    )
    form_state = {
        "entries": entries_raw,
        "entries_free": entries_free,
        "target": target,
        "top_k": top_k,
        "explain": explain,
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
        "explain": explain,
        "best": path_view(result["best"]) if result["best"] else None,
        "alternatives": [path_view(p) for p in result["alternatives"]],
        "entry_used": node_view(result["entry_used"]) if result["entry_used"] else None,
        "diagram": diagram_data(result, entries_raw),
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
