-- Dashboard datasets: the reviewable source of truth for what the dashboard asks.
--
-- The deployed copy of each query lives inside quest.lvdash.json, which is
-- machine-generated and effectively unreadable in a diff. These are kept here
-- so the queries themselves can be reviewed like any other SQL in the project.
-- Edit here first, then paste into the dashboard's dataset editor.
--
-- KNOWN LIMITATION - catalog and schema are written out literally below, unlike
-- every other .sql and .py in this project. Bundle variable substitution applies
-- to resource definitions, not to the contents of a referenced dashboard file,
-- so a prod dashboard needs its own export with prod names. Verify at deploy;
-- if substitution does happen, replace these with ${catalog}.${gold_schema}.
--
-- Every dataset reads only from the gold schema, so the dashboard works for a
-- read-only analyst who holds no privilege on silver or bronze.

-- ===========================================================================
-- Q1
-- ===========================================================================

-- q1_headline - one row, bound to counter widgets.
SELECT
  year_from,
  year_to,
  n_years,
  mean_population,
  stddev_sample,
  stddev_population
FROM rearc_quest_dev.dev_sachin_kamthankar_gold.q1_population_stats;


-- q1_population_by_year - the series behind the headline.
--
-- The year sequence and LEFT JOIN are deliberate. ACS published no 1-year
-- estimate for 2020, and joining the data to itself would simply omit that
-- year, leaving the chart to draw a straight line from 2019 to 2021 as though
-- nothing were missing. Generating the years first forces a NULL, so the gap
-- renders as a gap. in_q1_window marks the six years the headline average is
-- computed over, so the answer is visible in the context of the whole series.
WITH years AS (
  SELECT explode(sequence(2013, 2024)) AS year
)
SELECT
  y.year,
  p.population,
  y.year BETWEEN 2013 AND 2018 AS in_q1_window
FROM years y
LEFT JOIN rearc_quest_dev.dev_sachin_kamthankar_gold.population_current p
  ON p.year = y.year
ORDER BY y.year;


-- ===========================================================================
-- Q2
-- ===========================================================================

-- q2_best_year - the table, and the source for its filters.
--
-- units must be exposed as a filter, not merely as a column. Four quarters of
-- an index total around 400 and four quarters of a percent change around 14,
-- so an unfiltered table places the two scales side by side and invites a
-- comparison that means nothing. The filter is the finding, made unavoidable.
SELECT
  series_id,
  series_label,
  sector,
  measure,
  units,
  basis,
  best_year,
  total_value,
  n_periods
FROM rearc_quest_dev.dev_sachin_kamthankar_gold.q2_best_year_by_series
ORDER BY series_id;


-- q2_summary - counters. Optional: Lakeview can aggregate q2_best_year
-- directly, and one dataset is simpler than two. Kept explicit so the
-- 237 / 45 split is stated rather than derived in a widget config.
SELECT
  count(*)                                              AS series_total,
  count_if(basis = 'quarterly')                         AS quarterly,
  count_if(basis = 'annual_q05')                        AS annual_q05,
  count(DISTINCT units)                                 AS distinct_units
FROM rearc_quest_dev.dev_sachin_kamthankar_gold.q2_best_year_by_series;


-- ===========================================================================
-- Q3
-- ===========================================================================

-- q3_value_and_population - two series on very different scales.
--
-- value runs roughly -8 to +8; population runs around 3.3e8. They need two
-- axes or two stacked charts - plotted together on one axis, the value series
-- is a flat line at zero. has_population exists so the 28 years with no ACS
-- figure can be shown as such rather than silently dropped by the chart.
SELECT
  year,
  value,
  population,
  population IS NOT NULL AS has_population
FROM rearc_quest_dev.dev_sachin_kamthankar_gold.q3_series_value_and_population
ORDER BY year;


-- q3_context - the series being reported, for the panel title.
SELECT DISTINCT
  series_id,
  series_label,
  units,
  period
FROM rearc_quest_dev.dev_sachin_kamthankar_gold.q3_series_value_and_population;


-- ===========================================================================
-- Data quality
-- ===========================================================================

-- dq_checks - one row per check, every failing_rows must read 0.
-- A single counter bound to max(failing_rows) is the whole-dashboard signal.
SELECT
  check_name,
  failing_rows
FROM rearc_quest_dev.dev_sachin_kamthankar_gold.data_quality
ORDER BY check_name;


-- dq_scale - informative counts, deliberately without expected values.
-- These grow when BLS publishes; treating growth as a defect would make the
-- dashboard cry wolf. Shown as context, never as pass/fail.
SELECT
  (SELECT count(*) FROM rearc_quest_dev.dev_sachin_kamthankar_gold.pr_observations_current) AS current_observations,
  (SELECT count(DISTINCT series_id) FROM rearc_quest_dev.dev_sachin_kamthankar_gold.pr_observations_current) AS series,
  (SELECT count(*) FROM rearc_quest_dev.dev_sachin_kamthankar_gold.population_current) AS population_years;
