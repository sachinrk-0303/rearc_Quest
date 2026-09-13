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
