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
