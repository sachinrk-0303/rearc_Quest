# Rearc Data Quest — Databricks Edition

BLS productivity time series and ACS population estimates, ingested idempotently
from their live sources and modelled Bronze → Silver → Gold as a single Spark
Declarative Pipeline, deployed entirely through a Databricks Asset Bundle.

**Why** each decision was made is in **[PROCESS.md](PROCESS.md)** — including the
findings that changed the answers. This file covers what it is and how to run it.

---

## The answers

**Q1 — mean and standard deviation of the annual US population, 2013–2018**

| | |
|---|---|
| Mean | **322,069,808** |
| Standard deviation (sample, *n−1*) | **4,158,441.0409** |
| Standard deviation (population, *n*) | 3,796,119.9369 |

The question does not say which standard deviation it wants and they differ by
~9% over six points, so both are published. The sample form is the headline: ACS
figures are *estimates*, and six years are a sample of a longer series.

**Q2 — best year per series** (`gold.q2_best_year_by_series`, 282 rows)

| series_id | best_year | value | basis |
|---|---|---|---|
| `PRS30006011` | **2022** | **16.400** | quarterly |
| `PRS30006032` | **1994** | **14.600** | quarterly |

Those two are the only externally published checks in the quest, and both
reproduce exactly. `PRS30006032` only lands on 1994 if you read `AllData` rather
than `Current` **and** exclude `Q05`; a partial implementation gets 2021 and
looks perfectly reasonable.

> **Sums compare only within a `units` value.** Four quarters of an index total
> ≈ 400; four quarters of a percent change ≈ 14. `units` is in the output for
> exactly this reason.

`basis` distinguishes the 237 series scored on Q01–Q04 from the 45 that publish
no quarterly data at all and are scored on the annual average.

**Q3 — one series, one period, with population where available**
(`gold.q3_series_value_and_population`, 39 rows)

`PRS30006032` / `Q01` spans 1988–2026. Eleven of those years have an ACS
population: 2013–2019 and 2021–2024. There is no 2020 — the Census Bureau
suspended 1-year estimates for the pandemic year — and years with no population
are kept, not dropped.

---

## How it fits together

```
download.bls.gov/pub/time.series/pr/        api.datausa.io  (ACS 1-year cube)
              │                                      │
              └───────────── ingest job ─────────────┘
                             │            filenames discovered, never hardcoded
                             │            HEAD before GET, SHA-256 decides
                             ▼
     /Volumes/<catalog>/raw/landing/**/ingest_date=YYYY-MM-DD/
                             │
                             ▼   one Spark Declarative Pipeline
    bronze   13 tables   what arrived, with provenance; contracts enforced
    silver    5 tables   typed, quarantined, AUTO CDC SCD Type 2
    gold      4 tables   the three answers + a DataFrame API parity check
```

Re-running the ingest job after a full first run transfers **2,748 bytes** — the
BLS directory listing, which discovery must always fetch, plus the population
payload, which offers no `Last-Modified` to check against.

---

## Running it

### Prerequisites

- A Databricks workspace with Unity Catalog. Built and tested on **Free Edition**
  (serverless only).
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) ≥ 1.16
- [uv](https://docs.astral.sh/uv/) — only for the local test suite

### One-time setup

1. **Create the catalogs through the workspace UI**: `rearc_quest_dev` and
   `rearc_quest_prod`. This step is deliberately *not* in the bundle — under
   Default Storage the API refuses catalog creation both ways. See
   [PROCESS.md §3](PROCESS.md). Everything beneath them — schemas, volume,
   tables — is owned by the bundle.

2. **Point the bundle at your workspace.** In [`databricks.yaml`](databricks.yaml),
   set `workspace.host` under both targets, and set `contact_email` to your own
   address — BLS returns **403** to requests carrying no contact information,
   and the User-Agent deliberately identifies rather than impersonates.

3. **Authenticate.** The host alone resolves credentials; there is no `profile:`
   field to keep in sync.

   ```powershell
   databricks auth login --host https://<your-workspace>.cloud.databricks.com
   ```

### Deploy and run

```powershell
databricks bundle validate
databricks bundle deploy
databricks bundle run ingest       # lands source files into the Volume
databricks bundle run medallion    # builds Bronze -> Silver -> Gold
```

Add `-t prod` for the production target. `dev` is the default and applies a
`dev_<user>_` schema prefix, so two people can deploy the same bundle without
overwriting each other.

After changing a **column type**, a plain run fails with
`CANNOT_UPDATE_TABLE_SCHEMA` — Delta cannot merge incompatible types into a live
table:

```powershell
databricks bundle run medallion --full-refresh-all
```

That also resets the Auto Loader checkpoints and rebuilds every table from the
landed files alone, which is how reproducibility was verified rather than
assumed.

### Query the answers

```sql
SELECT * FROM rearc_quest_prod.gold.q1_population_stats;
SELECT * FROM rearc_quest_prod.gold.q2_best_year_by_series ORDER BY series_id;
SELECT * FROM rearc_quest_prod.gold.q3_series_value_and_population ORDER BY year;
SELECT * FROM rearc_quest_prod.gold.parity_check;
```

On the `dev` target the schemas carry the `dev_<user>_` prefix. No `.py`, `.sql`
or `.yml` in this repo contains a literal catalog or schema name — every one is
resolved from the bundle at runtime.

---

## Layout

```
databricks.yaml              bundle: variables, dev and prod targets
resources/
  catalog.yml                four schemas + the landing Volume
  ingest_job.yml             spark_python_task, weekly schedule
  pipeline.yml               serverless pipeline, sources, configuration
src/
  run_ingest.py              job entry point
  ingest/                    pure Python, no Spark - runs locally too
    manifest.py              hashing, change decisions, ManifestStore protocol
    landing.py               land vs refresh-metadata-only
    bls.py                   discovery, contact UA, HEAD-before-GET
    population.py            Tesseract fetch, truncation guard
    delta_manifest.py        Delta-backed ManifestStore
    main.py                  orchestration, tombstones, argparse
  pipelines/                 <layer>_<subject>, one subject per file
    bronze_observations.py   bronze_lookups.py   bronze_population.py
    silver_observations.py   silver_population.py   silver_series.py
    gold_questions.sql       the three answers, Spark SQL
    gold_parity.py           DataFrame API cross-check
tests/                       27 tests, no network and no Spark
PROCESS.md                   decisions, findings, trade-offs, AI usage
```

---

## How it is verified

**Locally** — no Databricks connection needed:

```powershell
uv run pytest -q                    # 27 tests
uv run ruff check src tests
```

The tests pin the change-detection logic: canonical hashing is order-insensitive
*and* raw hashing is not, so the contrast documents why the function exists; a
republished-but-identical file refreshes its metadata and writes nothing.

**In the pipeline**, continuously:

- **Header and column contracts** are `expect_or_fail`. Bronze splits by
  position and Silver selects by name, so a reordered or renamed column would
  corrupt every row while the field count still looked correct. These halt the
  run instead.
- **Quarantine tables** hold rows Silver will not vouch for. Both are currently
  empty — which is a measurement, not an assumption.
- **`gold.parity_check`** compares each SQL answer against an independent
  DataFrame API implementation and fails on any differing row.

The parity check is worth an honest caveat: two implementations by one author
catch transcription and dialect errors, not a misreading of the question. The
only real check on the interpretation is the two published values above.
