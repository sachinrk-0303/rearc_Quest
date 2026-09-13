-- Gold: current-row serving views.
--
-- Silver is SCD Type 2, so it stores one row per *version* of a key. Today
-- every row is current and the distinction is invisible - but the moment BLS
-- revises a figure, a key acquires a second row and any query that forgets
-- `__END_AT IS NULL` starts double-counting. It returns a larger number, not an
-- error, which is how every other trap in this dataset works.
--
-- Gold's own queries filter correctly. These views exist for everyone else: a
-- dashboard, a Genie space, an analyst writing ad-hoc SQL. Pointing them at
-- `_current` makes the mistake unavailable rather than merely documented.
--
-- Plain views, not materialized ones. A materialized view here would be a
-- second physical copy of 77,126 rows that has to be kept in step; a plain view
-- stores nothing and is resolved at read time, so it cannot disagree with the
-- table it summarises. A current-row filter is a definition, not a computation.
--
-- They live in the *gold* schema rather than beside their sources, and that is
-- an access-control decision rather than a cosmetic one. A Unity Catalog view
-- runs with its owner's privileges, so an analyst granted SELECT on gold can
-- read these without any grant on silver - no reach into SCD Type 2 history,
-- the quarantine tables, or Bronze. The view is the boundary.
--
-- Note: pipeline-created views cannot carry COMMENT or CONSTRAINT clauses, so
-- the descriptions that would help a Genie space live on the Gold tables
-- instead. The column meanings are documented in silver_observations.py and
-- silver_population.py.

CREATE OR REPLACE VIEW ${catalog}.${gold_schema}.pr_observations_current AS
SELECT
  series_id,
  year,
  period,
  value,
  footnote_codes
FROM ${catalog}.${silver_schema}.pr_observations
WHERE __END_AT IS NULL;


CREATE OR REPLACE VIEW ${catalog}.${gold_schema}.population_current AS
SELECT
  year,
  population,
  nation
FROM ${catalog}.${silver_schema}.population
WHERE __END_AT IS NULL;


-- Every check that must read zero, in one place.
--
-- This view exists for a single reason: the quarantine tables live in silver,
-- which a read-only analyst cannot see, so a dashboard built on them would
-- work for its author and fail for everyone it was shared with. Surfacing the
-- counts through gold uses the same owner's-privileges property as the views
-- above - the numbers become visible without the tables behind them becoming
-- readable.
--
-- Only pass/fail checks belong here, never scale. An earlier draft included
-- "series answered = 282" and "current observations = 77,126" as expected
-- values, which would turn the dashboard red the day BLS publishes a new
-- series - reporting growth in the source as a defect in the pipeline. Counts
-- that are merely informative are queried directly by the dashboard instead.
CREATE OR REPLACE VIEW ${catalog}.${gold_schema}.data_quality AS
SELECT
  'Observations quarantined' AS check_name,
  count(*)                   AS failing_rows
FROM ${catalog}.${silver_schema}.pr_observations_quarantine

UNION ALL
SELECT
  'Population rows quarantined',
  count(*)
FROM ${catalog}.${silver_schema}.population_quarantine

UNION ALL
SELECT
  concat('SQL vs PySpark: ', question),
  differing_rows
FROM ${catalog}.${gold_schema}.parity_check;
