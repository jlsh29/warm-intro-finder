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

Exercises **16 scenarios** against built-in fixtures and synthetic
tempdir CSVs: disconnected components, triangles, six-cycles, self-loops,
duplicate edges, ambiguous names, mixed reachable/unreachable entries,
`top_k` capping, entry-equals-target, hub-routing, identity merging
(Phase B), tiered scoring + shared-org derivation (Phase C), structured
JSON output (Phase D), and the Twitter ingester end-to-end (case 16).
Exits non-zero on any failure.

```bash
python run_stress_tests.py
```

The original 12 fixture cases use no `strength` column, so every edge
defaults to 1.0 — Dijkstra reduces to BFS and the original cases continue
to pass after every later upgrade.

---

## Platform adapters

Four standalone CLI converters turn real-world platform data into the
engine's three-CSV format (`people.csv`, `edges.csv`, `identities.csv`).
All four follow the same shape:

- **No network calls** — they read only the files you point at
- **No fabricated edges** — every emitted edge corresponds to evidence in the input
- **Namespaced person ids** — `tw_*` (Twitter), `li_*` (LinkedIn), `fc_*` (Farcaster), `wal_*` (wallet) — so outputs from multiple platforms can be combined without id collisions
- **Same output contract** — the three CSVs drop straight into `warm_intro.py` with no further processing

Pick the adapter for the platform whose data you have. Run it. Feed the
output to `warm_intro.py`. Or combine outputs from multiple adapters
into one merged dataset and let the engine's identity layer collapse
cross-platform duplicates (see [Cross-platform identity merging](#cross-platform-identity-merging) below).

### `twitter_ingester.py` — Twitter / X follower exports

Twitter follows are *directed*. Warm intros need mutual acquaintance, so
the ingester only emits an edge when the follow is bidirectional.

**Input** — one `following.csv` and one `followers.csv` per seed user.
Auto-detected handle column from `username | handle | screen_name |
user_screen_name | twitter_handle`; auto-detected display-name column
from `display_name | name | full_name | user_name`. Other columns
ignored. `@`-prefixes stripped, handles lowercased.

```csv
username,display_name
@bob,Bob Builder
@charlie,Charlie Chen
```

**Sample command:**
```bash
python twitter_ingester.py \
    --seed @alice \
    --following alice_following.csv --followers alice_followers.csv \
    --seed @bob \
    --following bob_following.csv --followers bob_followers.csv \
    --out-dir ./tw_data \
    --tier mutual
```

| Flag | Default | Notes |
|---|---|---|
| `--seed` | required | Twitter handle of the export owner. Repeatable. |
| `--following` / `--followers` | required | One pair per `--seed`, in the same order. |
| `--out-dir` | `.` | Where the three output CSVs are written. |
| `--include-one-way` | off | Also emit one-way follows at `platform_similarity` (mutual evidence still wins on dedup). |
| `--tier` | `platform_similarity` | Tier label for mutual edges. A bare follow is weak signal; default is conservative. Use `mutual` if you trust your follow graph. |

**Person ids:** `tw_<lowercase_handle>` (e.g. `tw_alice`).
**Output:** `people.csv` (id, name), `edges.csv` (from, to, strength, tier), `identities.csv` (person_id, twitter, @handle).

### `linkedin_ingester.py` — LinkedIn Connections.csv exports

LinkedIn connections are *inherently mutual* (both parties accepted), so
no follower/following split. One `Connections.csv` per export owner.

**Input** — LinkedIn's official export columns (`First Name, Last Name,
URL, Email Address, Company, Position, Connected On`) or the snake_case
equivalents from third-party tools. Auto-detected:

| field | aliases |
|---|---|
| URL | `url`, `profile_url`, `linkedin_url`, `profile` |
| first | `first_name`, `firstname`, `first` |
| last | `last_name`, `lastname`, `last` |
| company | `company`, `company_name`, `current_company`, `organization` |
| position | `position`, `title`, `job_title`, `role` |

Email Address is intentionally ignored (PII not needed for routing).

```csv
First Name,Last Name,URL,Email Address,Company,Position,Connected On
Charlie,Chen,https://www.linkedin.com/in/charlie-chen/,,Helix Labs,Senior Engineer,12 Mar 2024
```

**Sample command:**
```bash
python linkedin_ingester.py \
    --owner-name "Alice Anderson" \
    --owner-url  "https://www.linkedin.com/in/alice-anderson/" \
    --connections alice_connections.csv \
    --owner-name "Bob Builder" \
    --owner-url  "https://www.linkedin.com/in/bob-builder/" \
    --connections bob_connections.csv \
    --out-dir ./li_data
```

| Flag | Default | Notes |
|---|---|---|
| `--owner-name` / `--owner-url` / `--connections` | required | One triple per export owner. |
| `--out-dir` | `.` | |
| `--tier` | `mutual` | A LinkedIn connection is bidirectional acceptance — stronger evidence than a Twitter follow. |

**Person ids:** `li_<slug>` derived from the LinkedIn URL `/in/<slug>` path. Bare slugs accepted as fallback.
**Output:** `people.csv` (id, name, **company, role**), `edges.csv`, `identities.csv` (person_id, linkedin, full LinkedIn URL).

LinkedIn data uniquely populates `company` and `role` columns in
`people.csv`, so the engine's `--explain` flag shows full org context
on every node.

### `farcaster_ingester.py` — Farcaster follows + channels

Like Twitter — follows are directed and only mutuals are emitted by
default — but with two key differences:

- Person ids use the **immutable numeric FID**, not the mutable @username
- Optional `--channels` CSV adds `shared_org`-tier edges from channel co-membership

**Input** — per-seed following / followers files plus an optional
channels file.

| field | aliases |
|---|---|
| FID | `fid`, `farcaster_id`, `user_fid`, `id` |
| username | `username`, `handle`, `fname` |
| display name | `display_name`, `name`, `display` |
| channel | `channel`, `channel_name`, `parent_url` |

```csv
fid,username,display_name
1002,bob,Bob Builder
1003,charlie,Charlie Chen
```

```csv
fid,channel
1001,/dev
1002,/dev
1009,/art
```

**Sample command:**
```bash
python farcaster_ingester.py \
    --seed 1001 \
    --following alice_following.csv --followers alice_followers.csv \
    --seed 1002 \
    --following bob_following.csv   --followers bob_followers.csv \
    --channels channels.csv \
    --out-dir ./fc_data
```

| Flag | Default | Notes |
|---|---|---|
| `--seed` | required | Numeric FID. Repeatable. |
| `--following` / `--followers` | required | One pair per `--seed`. |
| `--channels` | none | Optional `(fid, channel)` CSV. Each channel becomes a `shared_org` clique. Mutual evidence wins on dedup. |
| `--out-dir` | `.` | |
| `--include-one-way` | off | |
| `--tier` | `mutual` | Farcaster follows are more deliberate than Twitter's. |

**Person ids:** `fc_<numeric_fid>` (e.g. `fc_1001`). Username preserved in identities.csv as `@username`.
**Output:** `people.csv`, `edges.csv`, `identities.csv` (person_id, farcaster, @handle).

### `wallet_ingester.py` — blockchain wallet interactions

The most flexible adapter. Real-world identity behind a wallet is
unknown by default, so this ingester takes an explicit
**wallet → person mapping** and falls back to per-wallet anonymous
persons (`wal_<address>`) for unmapped wallets.

**Inputs** — one required interactions CSV plus one optional mapping CSV.

| field (interactions) | aliases |
|---|---|
| from | `from`, `from_wallet`, `from_address`, `sender`, `source` |
| to | `to`, `to_wallet`, `to_address`, `receiver`, `recipient`, `target` |
| count | `count`, `tx_count`, `interaction_count`, `n` |
| type | `type`, `interaction_type`, `tx_type` (optional, ignored for routing) |

```csv
from_wallet,to_wallet,count,type
0x1111...,0x3333...,5,transfer
0x2222...,0x3333...,2,transfer
```

```csv
person_id,wallet,person_name
alice,0x1111...,Alice Anderson
alice,0x2222...,Alice Anderson
bob,0x3333...,Bob Builder
```

**Sample command:**
```bash
python wallet_ingester.py \
    --interactions interactions.csv \
    --mapping mapping.csv \
    --out-dir ./wal_data \
    --mutual-threshold 3
```

| Flag | Default | Notes |
|---|---|---|
| `--interactions` | required | Pre-aggregated wallet pairs (or use `count=1` per row for raw events). |
| `--mapping` | none | Optional `(person_id, wallet, [person_name])` CSV. Multiple wallets per person allowed (cross-wallet ownership). |
| `--out-dir` | `.` | |
| `--mutual-threshold` | `3` | Combined bidirectional count `≥ N` → `mutual` (8); else `platform_similarity` (2). |
| `--tier` | none | Override to force a uniform tier. |

**Person ids:** mapped wallets use the mapping's `person_id` as-is (no prefix); unmapped wallets become `wal_<lowercase_address>` with display name `0xabcd...1234`.

**Cross-wallet aggregation:** if Alice owns 3 wallets, every interaction between any of them and Bob is summed into one `(alice, bob)` count before tier classification. EVM addresses are lowercased; other formats pass through unchanged.

**Output:** `people.csv`, `edges.csv`, `identities.csv` (person_id, wallet, full address — every wallet appears, including those owned by mapped persons).

### Plugging adapter output into the engine

Every adapter writes the same three files. Feed them to `warm_intro.py`
exactly like the synthetic dataset:

```bash
python warm_intro.py \
    --people    fc_data/people.csv \
    --edges     fc_data/edges.csv \
    --identities fc_data/identities.csv \
    --entry fc_1001 --target fc_1003 --explain
```

The `--identities` flag is what triggers Phase B identity merging — when
two persons share a platform handle, the engine collapses them onto one
canonical person before routing.

### Cross-platform identity merging

Each adapter uses its own person-id namespace prefix (`tw_*`, `li_*`,
`fc_*`, `wal_*`) so outputs can be combined without id collisions:

1. Run each adapter independently to its own out-dir
2. Concatenate the three CSV families:
   ```bash
   tail -n +2 -q tw_data/people.csv li_data/people.csv fc_data/people.csv wal_data/people.csv \
       | sort -u > merged/people.csv  # add header back manually
   # similar for edges.csv and identities.csv
   ```
3. Add a manual identities row whenever you have evidence that two
   namespaced ids are the same human:
   ```csv
   person_id,platform,handle
   tw_alice,twitter,@alice_canonical
   li_alice-anderson,twitter,@alice_canonical
   ```
   The `SharedAccountResolver` will merge `tw_alice` and `li_alice-anderson`
   on graph load because they both claim the same `twitter:@alice_canonical`
   account. Same trick works with wallet addresses (`wallet:0xabc...`),
   ENS names, or any other platform identifier.

The merged graph then routes through the unified social fabric — your
LinkedIn-known coworker can introduce you to your Farcaster mutual via
their shared wallet activity, all in one query.

### Generic data hygiene tips
- **Normalize before load.** Adapters lowercase EVM addresses and strip
  `@` from handles, but anything you write directly to `people.csv`
  needs the same treatment.
- **One edge row per relationship.** Both directions of the same edge
  is fine — the engine dedupes (max strength wins) — but avoid emitting
  partial-strength rows that should be aggregated upstream.
- **Prune dangling edges.** Edges referencing people not in `people.csv`
  are silently skipped with a stderr warning. Adapters do this for you.
- **Privacy.** Everything runs locally. No adapter contacts an API,
  indexer, or platform.

### Scale
In-memory adjacency list. Tested up to the low thousands of nodes with
no measurable latency. Beyond ~50k nodes, consider porting to NetworkX,
graph-tool, or a graph database.

---

## Project layout

```
warm_intro/
├── warm_intro.py          # the CLI (Dijkstra-based pathfinder)
├── core.py                # domain model + GraphRepository / CSVRepository
├── identity.py            # IdentityResolver + ManualCSV / SharedAccount / Heuristic
├── app.py                 # Flask web UI
├── templates/
│   └── index.html         # UI template (vis-network path diagram)
├── generate_dataset.py    # synthetic data generator (with tier-strengths)
├── analyze.py             # connector/introducer analysis
├── run_stress_tests.py    # 16-case assertion suite
│
├── twitter_ingester.py    # Twitter follower/following CSV  -> engine CSVs
├── twitter_sample/        #   sample inputs + generated output
├── linkedin_ingester.py   # LinkedIn Connections.csv         -> engine CSVs
├── linkedin_sample/
├── farcaster_ingester.py  # Farcaster follows + channels     -> engine CSVs
├── farcaster_sample/
├── wallet_ingester.py     # Blockchain wallet interactions   -> engine CSVs
├── wallet_sample/
│
├── people.csv             # generated or your own
├── edges.csv              # generated or your own (with optional strength)
├── identities.csv.example # sample person <-> account mapping
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
