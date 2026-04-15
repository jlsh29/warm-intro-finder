# Warm Introduction Path Finder

A relationship-routing engine. Given a social graph and one or more entry
points, it finds the best warm-introduction path to a target person — the
chain of mutual connections you'd use to get introduced.

- **Algorithm:** multi-source Dijkstra over an undirected graph with
  `cost = 1 / strength`. With uniform strengths it reduces to BFS.
- **Inputs:** two (or three) CSV files — `people.csv`, `edges.csv`,
  and optional `identities.csv`.
- **Identity model:** every person is identified by a single
  platform-based username in the form `tw_<handle>` / `fc_<handle>` /
  `li_<slug>` / `wal_<0x...>` / `unknown_user_<N>`. No real names,
  companies, roles, or teams are stored or displayed.
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
| `id` | **yes** | Platform-namespaced username or wallet address. One of: `tw_<twitter_handle>`, `fc_<farcaster_handle>`, `li_<linkedin_slug>`, `wal_<0x...>`, or `unknown_user_<N>`. Must be globally unique. |
| `name` | optional | Legacy display-name column. Omitted from generated data; still honored if present (e.g. when you bring your own enriched CSV). |
| `company` / `team` / `role` | optional | Same — honored if present, ignored if absent. Surfaced by `--explain` only when non-empty. |

Extra columns are ignored. Duplicate `id` values cause an error.

**After the username-only transform the default generator emits only the `id` column:**

```csv
id
tw_orla_dutta
fc_sana
li_omar-erikson
wal_0xabcd...1234
unknown_user_1
```

The engine's `label()` falls back to `id` when `name` is absent, so
routing output reads like `tw_orla_dutta -(8|direct)-> fc_sana` with
no real names anywhere.

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
tw_orla_dutta,tw_giulia_tanaka,8
tw_giulia_tanaka,tw_gabriela_vargas,10
tw_orla_dutta,fc_bruno,2
```

### `identities.csv` — one row per person, priority platform only

After the username-only transform, each non-`unknown_user` person has
exactly one row listing the platform that determined their `id`.

| column | required | notes |
|---|---|---|
| `person_id` | **yes** | Matches an `id` in `people.csv`. |
| `platform` | **yes** | One of `twitter`, `farcaster`, `linkedin`, `wallet`. |
| `handle` | **yes** | Platform-specific handle (Twitter keeps `@`, others bare). |
| `dm` | optional | `yes` / `no` — is this handle DM-reachable? Surfaced as a badge in the Flask UI. |
| extra columns | — | Preserved into `SocialAccount.attributes` for downstream tools. |

```csv
person_id,platform,handle,dm
tw_orla_dutta,twitter,@orla_dutta,yes
fc_sana,farcaster,sana,no
li_omar-erikson,linkedin,omar-erikson,yes
wal_0xabcd...1234,wallet,0xabcd...1234,no
```

Loading `identities.csv` enables Phase B identity merging: when two
person ids both claim ownership of the same account (same
`platform:handle` pair), the engine collapses them onto one canonical
person before routing.

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
  tw_orla_dutta -(8|direct)-> tw_bruno_beck -(10|direct)-> tw_bruno_nakamura -(6|direct)-> tw_divya_moreau
Entry point used: tw_orla_dutta

Alternative paths (1):
  [4 hops, strength 36] tw_vera_okafor -(10|direct)-> tw_felix_moreau -(8|direct)-> ...

Why: Best warm path has 3 hop(s) with total strength 24 (cost 0.342),
routed through entry point tw_orla_dutta. Selected via multi-source
Dijkstra on an undirected graph with edge cost = 1/strength; 1
alternative path(s) of the same total cost were found.
```

`--explain` additionally surfaces any `company` / `team` / `role` columns
from `people.csv` in `[brackets]` after each person. The default generated
dataset has no such columns, so `--explain` is a no-op on it — the flag
is there for users who bring their own enriched data.

### Output (JSON, via `--output`)

```json
{
  "reachable": true,
  "hops": 3,
  "cost": 0.342,
  "total_strength": 24.0,
  "entry_used": { "id": "tw_orla_dutta", "name": "tw_orla_dutta" },
  "best_path": {
    "ids": ["tw_orla_dutta", "tw_bruno_beck", "tw_bruno_nakamura", "tw_divya_moreau"],
    "nodes": [
      { "id": "tw_orla_dutta", "name": "tw_orla_dutta" },
      { "id": "tw_bruno_beck", "name": "tw_bruno_beck" },
      { "id": "tw_bruno_nakamura", "name": "tw_bruno_nakamura" },
      { "id": "tw_divya_moreau", "name": "tw_divya_moreau" }
    ],
    "edges": [
      { "from": "tw_orla_dutta", "to": "tw_bruno_beck", "strength": 8.0, "cost": 0.125 },
      { "from": "tw_bruno_beck", "to": "tw_bruno_nakamura", "strength": 10.0, "cost": 0.1 },
      { "from": "tw_bruno_nakamura", "to": "tw_divya_moreau", "strength": 6.0, "cost": 0.167 }
    ],
    "hops": 3,
    "total_strength": 24.0,
    "total_cost": 0.392,
    "display": "tw_orla_dutta -(8|direct)-> tw_bruno_beck -> ..."
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
  --entry tw_orla_dutta --target tw_divya_moreau
```

**Multi-source, top-5 paths:**
```bash
python warm_intro.py --people people.csv --edges edges.csv \
  --entry tw_orla_dutta,fc_sana,li_omar-erikson --target tw_bella_xu --top-k 5
```

**Lookup by name (case-insensitive):**
```bash
# Only meaningful when people.csv has a `name` column. On the default
# generated data (id-only schema), pass ids directly as shown above.
python warm_intro.py --people enriched_people.csv --edges edges.csv \
  --entry "Orla Dutta" --target "Divya Moreau"
```

**JSON dump + identities (enables Phase B identity merging):**
```bash
python warm_intro.py --people people.csv --edges edges.csv \
  --identities identities.csv \
  --entry tw_orla_dutta,fc_sana --target tw_bella_xu --top-k 5 \
  --output result.json
```

---

## The web UI — `app.py`

A single-page Flask app for browsing the same query interface visually.

```bash
# Without identities.csv:
python app.py

# With identities.csv for social-link badges on each person card:
WARM_INTRO_IDENTITIES=identities.csv python app.py

# Then open http://127.0.0.1:5000/
```

Features:

- Multi-select dropdown for entry points plus a free-text fallback for
  comma-separated ids
- Dropdown for the target
- `top-k` numeric input and a submit button
- Results render as a horizontal chain of node cards with arrows between
  them; entry highlighted in blue, target highlighted in orange
- Best path tagged **BEST**, each alternative tagged **ALT 1**, **ALT 2**, …
- When `identities.csv` is loaded, each person card shows a clickable
  link to their priority-chosen platform (Twitter → `twitter.com/...`,
  LinkedIn → `linkedin.com/in/...`, Farcaster → `warpcast.com/...`,
  wallet → `debank.com/profile/...`) with a green `DM ✓` or grey
  `DM ✗` badge indicating availability. `unknown_user_*` rows render
  plain with no link row.
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

### `generate_dataset.py` — synthetic social graph

Generates a seeded 100-person, 200-edge network. Each person is
assigned a **single canonical platform identity** via priority
resolution:

1. Twitter (probability 0.70) → id is `tw_<handle>`
2. else Farcaster (0.60) → `fc_<handle>`
3. else LinkedIn (0.50) → `li_<slug>`
4. else wallet (0.30) → `wal_<0x...>`
5. else `unknown_user_<N>`

Independent coin flips per platform mean most people have a Twitter
handle available, a smaller share roll only Farcaster, fewer roll
only LinkedIn, fewer still only a wallet, and a handful roll nothing.
On seed 42 the default distribution is ~72/15/7/0/6 respectively.

Edge density and strengths follow three relationship tiers:

| tier | density | strength range |
|---|---|---|
| intra-team (close coworkers) | 60% | 7–10 |
| intra-company cross-team | 15% | 4–6 |
| cross-company (ex-colleagues, conferences) | 25% | 1–3 |

Team and company are tracked *internally* to produce realistic edge
clusters but are **never written to the output CSVs** — the
generated `people.csv` has only an `id` column.

```bash
python generate_dataset.py
# writes people.csv, edges.csv, and identities.csv in the working directory
```

Seed is `SEED=42` by default; change it in the script for variation.
`identities.csv` contains exactly one row per non-`unknown_user`
person: the priority-chosen platform handle with a random `dm`
(yes/no) flag.

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
**Output:** `people.csv` (id only), `edges.csv` (from, to, strength, tier), `identities.csv` (person_id, twitter, @handle).

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
**Output:** `people.csv` (id only), `edges.csv`, `identities.csv` (person_id, linkedin, full LinkedIn URL).

Company and role signals present in the source `Connections.csv` are
read by the auto-detector (so the column-name match works on any
third-party export shape) but **not** preserved in the output CSVs —
the system only routes on platform-based identity.

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
**Output:** `people.csv` (id only), `edges.csv`, `identities.csv` (person_id, farcaster, @handle).

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

**Output:** `people.csv` (id only), `edges.csv`, `identities.csv` (person_id, wallet, full address — every wallet appears, including those owned by mapped persons).

### Plugging adapter output into the engine

Every adapter writes the same three files. Feed them to `warm_intro.py`
exactly like the synthetic dataset:

```bash
python warm_intro.py \
    --people    fc_data/people.csv \
    --edges     fc_data/edges.csv \
    --identities fc_data/identities.csv \
    --entry fc_1001 --target fc_1003
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
├── people.csv             # generated: id-only schema (username-based)
├── edges.csv              # generated (with strength + tier columns)
├── identities.csv         # generated: one row per person, priority platform
├── identities.csv.example # small hand-authored sample (pre-transform shape)
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
