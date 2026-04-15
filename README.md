# Warm Introduction Path Finder

A relationship-routing engine. Given a social graph and one or more entry
points, it finds the best warm-introduction path to a target person — the
chain of mutual connections you'd use to get introduced.

- **Algorithm:** multi-source Dijkstra over an undirected graph with
  `cost = 1 / strength`. With uniform strengths it reduces to BFS.
- **Inputs:** two CSV files (`people.csv`, `edges.csv`).
- **Interfaces:** CLI, Flask web UI, JSON export.
- **Runtime:** pure Python stdlib + Flask. Tested in-memory up to a few
  thousand nodes.

---

## Installation

Requires Python 3.9+. Only Flask needs to be installed (for the web UI;
the CLI itself is stdlib-only).

```bash
cd warm_intro
python -m pip install flask
python warm_intro.py --help
```

---

## CSV formats

### `people.csv` — one row per person

| column | required | notes |
|---|---|---|
| `id` | **yes** | Canonical, unique identifier (e.g. `p001`, a handle, a UUID). |
| `name` | optional | Display name. Used for lookup by name and in output. |
| `company` | optional | Surfaced by `--explain`. |
| `team` | optional | Surfaced by `--explain`. If prefixed with the company name plus ` / `, the prefix is stripped on display only. |
| `role` | optional | Surfaced by `--explain`. |

Extra columns are ignored. Duplicate `id` values cause an error. Duplicate
names are allowed but must be disambiguated by id at query time.

```csv
id,name,company,team,role
p001,Orla Dutta,Helix Labs,Helix Labs / Team 1,Engineer
p002,Wyatt Ito,Helix Labs,Helix Labs / Team 1,Engineering Manager
```

### `edges.csv` — one row per connection

| column | required | notes |
|---|---|---|
| `from` | **yes** | Person id. |
| `to` | **yes** | Person id. |
| `strength` | optional | Integer in `[1, 10]`. Higher = stronger tie. **Pathfinding prefers stronger edges** by minimizing `sum(1/strength)`. |

Edges are treated as **undirected**. Rules:

- Self-loops (`a,a`) are skipped silently.
- Duplicate edges are deduped (the higher of the two strengths wins).
- Edges referencing unknown ids are skipped with a stderr warning.
- If the `strength` column is **missing**, every edge defaults to
  `strength = 1.0` and the tool prints a warning that it's running in
  "unweighted mode" — equivalent to BFS.

```csv
from,to,strength
p001,p008,8
p008,p004,10
p001,p006,2
```

---

## Weighted vs unweighted pathfinding

The tool uses **multi-source Dijkstra**, with edge cost = `1 / strength`.
This has two important consequences:

1. **Stronger ties are preferred.** A 2-hop chain of strong edges
   (cost 0.1 + 0.1 = 0.2) beats a 1-hop weak edge (cost 1.0).
2. **Hop count is not the only factor.** A longer path of strong ties
   can win over a shorter path that includes a weak link. To prefer
   shortest paths regardless of strength, omit the `strength` column.

Output reports both `hops` and `total_strength` so you can see the
trade-off. The `cost` field is the raw Dijkstra distance
(`sum(1/strength)`); lower is better.

---

## The CLI — `warm_intro.py`

### Required flags

| flag | description |
|---|---|
| `--people PATH` | path to people.csv |
| `--edges PATH` | path to edges.csv |
| `--entry TOKENS` | comma-separated ids or names — one or more entry points you already have access to |
| `--target TOKEN` | id or name of the person you want to reach |

### Optional flags

| flag | default | description |
|---|---|---|
| `--top-k N` | 3 | how many minimum-cost paths to surface (best + up to N-1 alternatives) |
| `--output PATH` | — | write full result (best path, alternatives, edge metadata) as JSON |
| `--explain` | off | show `[Company / Team / Role]` after each person and one node per line |

### Lookup rules

Entry and target tokens can be either an `id` or a `name`:

- Exact `id` match wins first.
- Otherwise case-insensitive `name` match.
- Ambiguous names (e.g. two people both named "Echo") **raise an error** —
  the tool refuses to guess. Pass the id instead.

### Output (stdout)

```text
Best warm intro path (3 hop(s), total strength 24):
  Orla Dutta (p001) -(8)-> Bruno Beck (p006) -(10)-> Bruno Nakamura (p097) -(6)-> Divya Moreau (p100)
Entry point used: Orla Dutta (p001)

Alternative paths (1):
  [4 hops, strength 36] Vera Okafor (p020) -(10)-> Felix Moreau (p017) -(8)-> ...

Why: Best warm path has 3 hop(s) with total strength 24 (cost 0.342),
routed through entry point Orla Dutta (p001). Selected via multi-source
Dijkstra on an undirected graph with edge cost = 1/strength; 1
alternative path(s) of the same total cost were found.
```

With `--explain`:

```text
Best warm intro path (3 hop(s), total strength 24):
  Orla Dutta (p001) [Helix Labs / Team 1 / Engineer]
    -(8)-> Bruno Beck (p006) [Helix Labs / Team 1 / Engineer]
    -(10)-> Bruno Nakamura (p097) [Solstice Partners / Team 2 / Researcher]
    -(6)-> Divya Moreau (p100) [Solstice Partners / Team 2 / Founder]
```

### Output (JSON, via `--output`)

```json
{
  "reachable": true,
  "hops": 3,
  "cost": 0.342,
  "total_strength": 24.0,
  "entry_used": { "id": "p001", "name": "Orla Dutta" },
  "best_path": {
    "ids": ["p001", "p006", "p097", "p100"],
    "nodes": [
      { "id": "p001", "name": "Orla Dutta" },
      { "id": "p006", "name": "Bruno Beck" },
      { "id": "p097", "name": "Bruno Nakamura" },
      { "id": "p100", "name": "Divya Moreau" }
    ],
    "edges": [
      { "from": "p001", "to": "p006", "strength": 8.0, "cost": 0.125 },
      { "from": "p006", "to": "p097", "strength": 10.0, "cost": 0.1 },
      { "from": "p097", "to": "p100", "strength": 6.0, "cost": 0.167 }
    ],
    "hops": 3,
    "total_strength": 24.0,
    "total_cost": 0.392,
    "display": "Orla Dutta (p001) -(8)-> Bruno Beck (p006) -> ..."
  },
  "alternatives": [ /* same shape */ ],
  "explanation": "..."
}
```

When `--explain` is combined with `--output`, each node gains `company`,
`team`, and `role` fields, and `entry_used` gains the same.

Unreachable queries still write a well-formed JSON file with
`"reachable": false` and `null` path/cost fields — callers can
distinguish "no path" from "tool crashed".

### Exit codes

| code | meaning |
|---|---|
| 0 | success, path found |
| 1 | query ran but target is unreachable from all entries |
| 2 | input error (unknown/ambiguous person, bad CSV) |

---

## Command examples

**Simple query (BFS-equivalent if strength column is absent):**
```bash
python warm_intro.py --people people.csv --edges edges.csv \
  --entry alice --target zoe
```

**Multi-source, top-5 paths:**
```bash
python warm_intro.py --people people.csv --edges edges.csv \
  --entry p001,p020,p030 --target p100 --top-k 5
```

**Lookup by name (case-insensitive):**
```bash
python warm_intro.py --people people.csv --edges edges.csv \
  --entry "Orla Dutta" --target "Divya Moreau"
```

**Full context + JSON dump:**
```bash
python warm_intro.py --people people.csv --edges edges.csv \
  --entry p001,p020,p030 --target p100 --top-k 5 \
  --explain --output result.json
```

---

## The web UI — `app.py`

A single-page Flask app for browsing the same query interface visually.

```bash
python app.py
# then open http://127.0.0.1:5000/
```

Features:

- Multi-select dropdown for entry points (sorted by name) plus a free-text
  fallback for comma-separated ids/names
- Dropdown for the target
- `top-k` numeric input and an "Show company / team / role" checkbox
- Results render as a horizontal chain of node cards with arrows between
  them; entry highlighted in blue, target highlighted in orange
- Best path tagged **BEST**, each alternative tagged **ALT 1**, **ALT 2**, …
- Inline error banner for ambiguous names, unknown people, and missing inputs
- Shows an "UNREACHABLE" card when no path exists

The UI loads the graph once at startup. To point it at different CSVs
without editing code, set environment variables:

```bash
WARM_INTRO_PEOPLE=my_people.csv WARM_INTRO_EDGES=my_edges.csv python app.py
```

> **Note:** the current UI shows hop-count and node metadata; per-edge
> strength values are present in the JSON payload but not yet rendered as
> badges between nodes. Easy follow-up if useful.

---

## Companion scripts

### `generate_dataset.py` — synthetic professional network

Generates a seeded 100-person, 200-edge network across 6 companies with
realistic tiered edge density and **tiered strengths**:

| tier | density | strength range |
|---|---|---|
| intra-team (close coworkers) | 60% | 7–10 |
| intra-company cross-team | 15% | 4–6 |
| cross-company (ex-colleagues, conferences) | 25% | 1–3 |

```bash
python generate_dataset.py
# writes people.csv and edges.csv in the working directory
```

Seed is `SEED=42` by default; change it in the script for variation.
Summary now reports min/avg/max strength per tier.

### `analyze.py` — connector & introducer analysis

Reads `people.csv` + `edges.csv` and reports:

- **Top 10 most connected** (raw degree)
- **Cross-company bridges per company** (which companies have the widest
  external footprint)
- **Top 10 introducers** — composite score:
  `0.60 · betweenness + 0.25 · degree + 0.15 · cross-company neighbors`.
  Betweenness centrality is computed via Brandes' algorithm
  (unweighted, exact).

Also exports:
- `network_report.json` — structured for downstream tools
- `network_report.txt` — same tables as stdout

```bash
python analyze.py
```

This script ignores edge strengths — it ranks structural position, not
relationship intensity.

### `run_stress_tests.py` — assertion-based test suite

Exercises 12 scenarios against fixtures `test_people.csv` /
`test_edges.csv`: disconnected components, triangles, six-cycles,
self-loops, duplicate edges, ambiguous names, mixed reachable/unreachable
entries, `top_k` capping, entry-equals-target, and hub-routing with many
shortest paths. Exits non-zero on any failure.

```bash
python run_stress_tests.py
```

The fixture has no `strength` column, so every edge defaults to 1.0 —
Dijkstra reduces to BFS and all 12 cases continue to pass after the
weighted-edge upgrade.

---

## Using with real data

The tool is format-agnostic as long as your data collapses into two CSVs.

### LinkedIn / professional network exports
- Person id: LinkedIn URL slug or profile URL.
- `edges.csv`: one row per accepted connection.
- Connections are mutual, so they're already undirected — no conversion.
- Strength signal options: years connected, profile-similarity, or shared
  endorsements. Map to integer `[1, 10]`.

### Twitter / X followers
- Follows are **directed**, but warm intros need mutual acquaintance.
  Keep only edges where both `A→B` and `B→A` exist; emit each as one
  undirected row:
  ```python
  mutual = {tuple(sorted((a, b))) for a, b in follows if (b, a) in follows}
  ```
- Strength signal options: number of replies/quote-tweets, follow
  duration, common follower count.

### CRM / email / Slack data
- Person id: email address.
- Edge signal options: at least N messages exchanged, both attended the
  same meeting, or both on the same thread.
- Strength: log of message volume, capped at 10. (Dijkstra rewards strong
  ties, but extreme outliers make every other edge look weak — log scale
  prevents one prolific Slacker from dominating every path.)

### Merging multiple sources
Run your extractors independently, concatenate the `edges.csv` files,
and let the tool's dedup handle overlap. Keep `id` canonical across
sources (always email address, or always LinkedIn slug) — mismatched
identifiers will fragment the graph.

When the same edge appears in two sources with different strengths, the
loader keeps the **higher** of the two — assuming a stronger signal in
either source represents a genuine tie.

### Data hygiene tips
- **Normalize before load.** Lowercase emails, strip handle whitespace.
- **One edge row per relationship.** A+B and B+A is fine — the tool
  dedupes — but avoid emitting strengths from multiple weak signals as
  separate rows; aggregate first, then emit one `strength` value.
- **Prune dangling edges.** Edges referencing people not in `people.csv`
  are silently skipped with a stderr warning. Fine for exploration;
  clean them up for production.
- **Privacy.** The tool runs entirely locally and reads only the files
  you point it at. No network calls.

### Scale
In-memory adjacency list. Tested up to the low thousands of nodes with
no measurable latency. Beyond ~50k nodes, consider porting to NetworkX,
graph-tool, or a graph database.

---

## Project layout

```
warm_intro/
├── warm_intro.py          # the CLI (Dijkstra-based pathfinder)
├── app.py                 # Flask web UI
├── templates/
│   └── index.html         # UI template
├── generate_dataset.py    # synthetic data generator (with strengths)
├── analyze.py             # connector/introducer analysis
├── run_stress_tests.py    # 12-case assertion suite
├── people.csv             # generated or your own
├── edges.csv              # generated or your own (with optional strength)
├── test_people.csv        # stress-test fixtures
├── test_edges.csv
├── network_report.json    # produced by analyze.py
├── network_report.txt
└── result.json            # produced by warm_intro.py --output
```

---

## Design notes

- **Undirected by design.** A warm intro requires the intermediate to
  know both endpoints. If your source data is directed (Twitter
  follows), collapse to mutual edges before feeding the tool.
- **Weighted shortest cost.** Dijkstra over `cost = 1/strength`
  minimizes total cost across the path. With uniform strengths it
  reproduces BFS hop-count behavior — so the 12 stress tests pass
  against weight-less fixtures.
- **All min-cost paths enumerated.** Predecessor sets are kept for every
  node at its minimum cost; alternatives are exhaustive up to `top_k`,
  using `math.isclose` to compare floating-point costs safely.
- **No fabricated relationships.** If a path doesn't exist, the tool
  reports unreachable. It never invents edges.
- **Ambiguity is an error.** Two people named "Echo"? The tool refuses
  to guess which one you meant.
- **Back-compat.** Old `edges.csv` files without a `strength` column
  work unchanged — the loader prints a warning and treats every edge as
  strength 1.0.
