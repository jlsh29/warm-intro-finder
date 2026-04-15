# Getting Started — Warm Introduction Path Finder

A step-by-step tutorial for using the warm intro finder from scratch.
This walks you through installation, your first query, the web UI, and
ingesting real data from Twitter, LinkedIn, Farcaster, or wallets.

> **For a complete reference**, see [README.md](README.md). This guide
> is the friendly walkthrough; the README is the dictionary.

---

## What does this tool do?

You have a network of people. You want to be introduced to someone in
that network, but you don't know them directly. The tool finds the
shortest "warm introduction path" — the chain of mutual connections
that gets you there.

Example: you want to reach **Divya Moreau** at Solstice Partners.
You don't know her. But you know **Orla Dutta** at Helix Labs, who
knows **Bruno Beck**, who knows **Bruno Nakamura** at Solstice, who
knows Divya. The tool finds that chain in milliseconds, even across
hundreds of people.

---

## Prerequisites

You need:

- **Python 3.9 or newer** — check with `python --version`
- A **terminal** — Command Prompt or PowerShell on Windows; Terminal on Mac/Linux
- **About 5 minutes** for the basic walkthrough

That's it. No database, no cloud account, no API keys.

---

## Step 1 — Get the code

```bash
git clone https://github.com/jlsh29/warm-intro-finder.git
cd warm-intro-finder
```

If you don't have `git`, you can download the zip from the GitHub page
and extract it.

**You'll know it worked when** `ls` (Mac/Linux) or `dir` (Windows)
shows files like `warm_intro.py`, `app.py`, and `README.md`.

---

## Step 2 — Install dependencies

The CLI tool itself uses only Python's standard library — nothing to
install. Only the **web UI** needs Flask:

```bash
python -m pip install -r requirements.txt
```

**You'll know it worked when** the last line says
`Successfully installed flask-3.x.x` (or shows that flask is already installed).

---

## Step 3 — Look at the sample data

The project ships with synthetic data: 100 people across 6 fictional
companies, with 200 connections of varying strengths. Open these in
any text editor or spreadsheet:

- `people.csv` — every person, their company, team, and role
- `edges.csv` — every connection between two people, with a strength score (1–10)

```
people.csv
id,name,company,team,role
p001,Orla Dutta,Helix Labs,Helix Labs / Team 1,Engineer
p002,Wyatt Ito,Helix Labs,Helix Labs / Team 1,Engineering Manager
...

edges.csv
from,to,strength
p001,p005,10
p001,p006,10
p001,p008,8
...
```

You don't need to understand every row — these are the inputs. The
tool reads them and figures out the rest.

---

## Step 4 — Your first query

Let's find a path from **Orla Dutta (p001)** to **Divya Moreau (p100)**.

```bash
python warm_intro.py --people people.csv --edges edges.csv --entry p001 --target p100 --top-k 3
```

You'll see something like:

```
Best warm intro path (8 hop(s), total strength 67):
  Orla Dutta (p001) -(8|direct)-> Giulia Tanaka (p008) -(10|direct)-> Gabriela Vargas (p004) -(10|direct)-> Wyatt Ito (p002) -(3|direct)-> Finn Gupta (p091) -(10|direct)-> Ishaan Xu (p092) -(6|direct)-> Rafael Nakamura (p096) -(10|direct)-> Bruno Nakamura (p097) -(10|direct)-> Divya Moreau (p100)
Entry point used: Orla Dutta (p001)

Why: Best warm path has 8 hop(s) with total strength 67 (cost 1.125), routed through entry point Orla Dutta (p001)...
```

**What this means:**
- **Hops** = how many handoffs the introduction chain has
- **Strength per edge** = how strong each individual connection is (1–10)
- **Total strength** = sum across the path. Higher is better.
- **Cost** = the math the tool optimizes (lower is better; equals `sum(1/strength)`)

**Try variations:**

```bash
# Add more entry points — the tool picks whichever gives the shortest path
python warm_intro.py --people people.csv --edges edges.csv --entry p001,p020,p030 --target p100

# Show company / team / role for every person on the path
python warm_intro.py --people people.csv --edges edges.csv --entry p001 --target p100 --explain

# Look up by name instead of id (case-insensitive)
python warm_intro.py --people people.csv --edges edges.csv --entry "Orla Dutta" --target "Divya Moreau"

# Save the result as a JSON file for downstream tools
python warm_intro.py --people people.csv --edges edges.csv --entry p001 --target p100 --output result.json
```

---

## Step 5 — Try the web UI

If you'd rather click than type, run the Flask UI:

```bash
python app.py
```

You'll see:

```
 * Running on http://127.0.0.1:5000
```

Open **http://127.0.0.1:5000** in your browser. You'll see:

1. A dropdown for **entry points** (people you already know — pick one or many)
2. A dropdown for the **target** person
3. A **Find warm path** button

Click submit and you'll get an interactive **path diagram**: nodes are
people, arrows are connections, hover any arrow to see the reason. The
best path is highlighted in blue; alternatives are dashed.

To stop the server, press `Ctrl+C` in the terminal.

---

## Step 6 — Use your own real data

The synthetic dataset is just for learning. Now let's plug in real data
from one of four sources.

### Step 6a — Twitter / X follower data

You'll need two CSV files **per seed user** (one person whose followers
you have access to):
- `following.csv` — accounts they follow
- `followers.csv` — accounts that follow them

The CSVs need at least a column with the username. Common formats work
out of the box:

```csv
username,display_name
@bob,Bob Builder
@charlie,Charlie Chen
```

Run the ingester:

```bash
python twitter_ingester.py \
    --seed @alice \
    --following alice_following.csv \
    --followers alice_followers.csv \
    --out-dir ./tw_data
```

Add more `--seed / --following / --followers` triples for additional
seed users. The tool will combine them and dedupe.

**It writes three files** to `./tw_data/`:
- `people.csv`, `edges.csv`, `identities.csv`

By default it only emits **mutual** follows (both A→B and B→A) because
warm intros need bidirectional acquaintance. Add `--include-one-way` to
also include one-way follows at a weaker strength.

Now feed those into the engine:

```bash
python warm_intro.py \
    --people tw_data/people.csv \
    --edges  tw_data/edges.csv \
    --identities tw_data/identities.csv \
    --entry tw_alice --target tw_charlie
```

> **Where do real Twitter CSVs come from?** Twitter's official archive
> doesn't include followers/following in CSV form anymore. Most users
> use third-party export tools or the API. As long as the CSV has a
> `username` (or `handle` / `screen_name`) column, the ingester will
> read it.

### Step 6b — LinkedIn connections

LinkedIn lets you download your connections directly:

1. Go to **LinkedIn Settings → Data Privacy → Get a copy of your data**
2. Choose **Connections** specifically (faster than the full archive)
3. Wait ~10 minutes for the email
4. Download and unzip — you'll get `Connections.csv`

This file has columns `First Name, Last Name, URL, Email Address, Company, Position, Connected On`.

Run the ingester:

```bash
python linkedin_ingester.py \
    --owner-name "Alice Anderson" \
    --owner-url  "https://www.linkedin.com/in/alice-anderson/" \
    --connections Connections.csv \
    --out-dir ./li_data
```

(Repeat the `--owner-name / --owner-url / --connections` triple for
each person whose connections you have.)

LinkedIn connections are **inherently mutual** (both parties accepted),
so all rows produce undirected edges. The default tier is `mutual`
(strength 8) — stronger evidence than a Twitter follow.

The output `people.csv` includes **company and role** columns, so:

```bash
python warm_intro.py \
    --people li_data/people.csv \
    --edges  li_data/edges.csv \
    --identities li_data/identities.csv \
    --entry li_alice-anderson --target li_charlie-chen --explain
```

…shows full job-title context on every person in the path.

### Step 6c — Farcaster follows + channels

Farcaster gives you two data sources:
- **Follows** — same shape as Twitter (following + followers)
- **Channels** — communities people belong to (`/dev`, `/art`, etc.)

Both use the user's **FID** (numeric Farcaster ID) as the canonical
identifier. Here's the format:

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

Run:

```bash
python farcaster_ingester.py \
    --seed 1001 \
    --following alice_following.csv \
    --followers alice_followers.csv \
    --channels channels.csv \
    --out-dir ./fc_data
```

Channel co-membership produces `shared_org`-tier edges (strength 5)
between every pair of channel members — useful when two people don't
follow each other but are active in the same community.

Person ids are `fc_<numeric_fid>` (e.g. `fc_1001`), not the @handle —
because handles can change but FIDs can't.

```bash
python warm_intro.py \
    --people fc_data/people.csv \
    --edges  fc_data/edges.csv \
    --identities fc_data/identities.csv \
    --entry fc_1001 --target fc_1009 --explain
```

> **Where do real Farcaster CSVs come from?** Use the
> [Neynar API](https://docs.neynar.com) or query a Farcaster Hub
> directly to pull a user's follows and channel memberships, then
> export to CSV. The free Neynar tier is generous.

### Step 6d — Blockchain wallet interactions

You need:
- **`interactions.csv`** — wallet-to-wallet activity, ideally pre-aggregated
- **`mapping.csv`** (optional) — which wallets belong to which person

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

Run:

```bash
python wallet_ingester.py \
    --interactions interactions.csv \
    --mapping mapping.csv \
    --out-dir ./wal_data \
    --mutual-threshold 3
```

`--mutual-threshold` controls how strong an interaction history needs
to be before it counts as a real social tie. Default 3 means: 3+
interactions between the same pair → `mutual` (strength 8); fewer →
`platform_similarity` (strength 2).

**Wallets without a mapping** become anonymous people with id
`wal_<address>`. They still appear in the graph and can show up in
introduction paths — you just don't know their real name yet.

**Where do real wallet CSVs come from?**
- [**Etherscan**](https://etherscan.io) → any address → "Export CSV"
  gives you all transactions for that wallet
- [**Dune Analytics**](https://dune.com) — write a SQL query, export
  results to CSV
- [**Covalent**](https://covalenthq.com) or **Alchemy** APIs — fetch
  programmatically

You'll need to pre-aggregate (one row per wallet pair with a count
column) before feeding the ingester.

### Step 6e — Combining multiple platforms

The really powerful move: run multiple ingesters and combine their
outputs into one merged graph. Because each ingester namespaces its
person ids (`tw_*`, `li_*`, `fc_*`, `wal_*`), the IDs won't collide.

1. Run each ingester to its own out-dir (`./tw_data`, `./li_data`, etc.)
2. Concatenate the CSVs (preserve one header, then append data rows from each)
3. Add a manual `identities.csv` row whenever you know two namespaced ids are the same person:

```csv
person_id,platform,handle
tw_alice,twitter,@alice_canonical
li_alice-anderson,twitter,@alice_canonical
fc_1001,twitter,@alice_canonical
```

Now when the engine loads, it sees three ids all claiming
`twitter:@alice_canonical` and **automatically merges them** into one
canonical person. Your LinkedIn-known coworker can introduce you to
your Farcaster mutual via their wallet activity, all in one query.

This is Phase B identity merging. See `core.py` and `identity.py` if
you want to understand what's happening under the hood.

---

## Step 7 — Get insights about your network

Once you have a `people.csv` and `edges.csv` (synthetic or real), run:

```bash
python analyze.py
```

You'll get:
- **Top 10 most connected people** — your network's hubs
- **Cross-company bridges by company** — which orgs have the widest reach
- **Top 10 introducers overall** — who can reach the most other people
  (uses betweenness centrality, not just raw connection count)

Two reports are saved: `network_report.json` (machine-readable) and
`network_report.txt` (same tables as stdout).

---

## Common problems

### `python: command not found`
Try `python3` instead. On some systems, `python` is Python 2 or not installed.

### `No module named 'flask'`
You skipped Step 2. Run `python -m pip install -r requirements.txt`.

### "Edge endpoint not found in people.csv"
You have an edge that references a person id that's not in `people.csv`.
The tool skips the edge with a warning. To fix: add the missing person,
or remove the bad edge.

### "Ambiguous name 'Echo'"
Two people in your data are both named "Echo" — the tool refuses to
guess which one you meant. Use the person's `id` (e.g. `n31`) instead
of their name.

### Path looks wrong / weaker than expected
The tool optimizes for **lowest cost**, not fewest hops. A 4-hop chain
of strong edges (strength 10 each) beats a 2-hop chain with one weak
edge (strength 2). If you want pure hop-count routing, omit the
`strength` column from `edges.csv` — every edge becomes weight 1 and
Dijkstra reduces to BFS.

### Web UI shows blank diagram
Make sure your browser can reach `unpkg.com` (the diagram library is
loaded from a CDN). Behind a corporate firewall? The card-based path
view below the diagram still works fully offline.

### Windows console shows weird characters
Some Windows terminals can't render certain Unicode arrows. The tool
already uses ASCII arrows (`->` instead of `→`) for compatibility. If
you still see `?` symbols, try running in PowerShell with `chcp 65001`
to switch to UTF-8.

---

## What's next?

- **Read [README.md](README.md)** for the full reference: every CLI
  flag, every CSV column, the JSON schema, design decisions, scaling
  notes
- **Look at [run_stress_tests.py](run_stress_tests.py)** for 16
  worked examples of edge cases the tool handles
- **Try [generate_dataset.py](generate_dataset.py)** to make different
  synthetic datasets (change `SEED` to vary the random graph)
- **Hack on [core.py](core.py) and [identity.py](identity.py)** to add
  your own platform adapter (the existing four are ~250-300 lines each)

The tool is intentionally small (~1500 lines total across the engine
plus four adapters) and stdlib-only for the engine itself. Read it
all in an afternoon.
