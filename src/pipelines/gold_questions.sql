-- Gold: the three quest answers.
--
-- Spark SQL rather than PySpark, because this is the layer people read. A
-- dashboard, a Genie space and an analyst looking for "how was this computed"
-- all land here, and SQL is the dialect all three already speak. The PySpark
-- equivalents exist as a parity check, not as the implementation.
--
-- ${catalog}, ${silver_schema} and ${gold_schema} are substituted by the
-- pipeline from its configuration block, so this file - like every .py in the
-- project - contains no literal catalog or schema name and runs unchanged
-- against dev and prod.
--
-- Every query filters __END_AT IS NULL. The Silver tables are SCD Type 2, so
-- omitting it would silently include superseded values once BLS or the Census
-- Bureau revises anything. There are no closed versions today, which is
-- precisely why the filter has to be written now: nothing would fail if it
-- were missing, until the day it matters.

-- Q1: mean and standard deviation of the annual US population, 2013-2018.
--
-- Both standard deviations are emitted because the question does not say
-- which it wants, and they differ by about 9% on six data points. The sample
-- form (dividing by n-1) is the headline answer: these are ACS *estimates* of
-- the population, not a census of it, and six annual estimates are a sample of
-- a longer series rather than the entire universe of interest.
--
-- Note the word "population" carries two unrelated meanings in this table.
-- mean_population and the year columns describe people; stddev_sample and
-- stddev_population name the two statistical formulas - n-1 and n.
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.q1_population_stats
(
  CONSTRAINT all_six_years_present EXPECT (n_years = 6) ON VIOLATION FAIL UPDATE
)
COMMENT 'Q1. Mean and standard deviation of annual US population, 2013-2018 inclusive. Sample standard deviation is the headline answer; both are shown.'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  min(year)                AS year_from,
  max(year)                AS year_to,
  count(*)                 AS n_years,
  avg(population)          AS mean_population,
  stddev_samp(population)  AS stddev_sample,
  stddev_pop(population)   AS stddev_population
FROM ${catalog}.${silver_schema}.population
WHERE __END_AT IS NULL
  AND year BETWEEN 2013 AND 2018;


-- Q2: for each series, the year whose values sum highest.
--
-- Q05 is excluded from the quarterly sum. pr.period names it "Annual Average"
-- and pr.txt section 3 says the same, so including it alongside Q01-Q04 adds
-- the year to itself. That single choice moves PRS30006011 from 16.4 to 20.5
-- and PRS30006032 from 14.6 to 17.2 - both perfectly plausible numbers, both
-- wrong, and neither of them raising an error.
--
-- The 45 series that publish *only* Q05 are still answered, not dropped. They
-- carry basis = 'annual_q05' and are ranked on the annual average itself.
-- Dropping them would lose 16% of the series with nothing to show for it;
-- ranking them on quarters they do not have would invent data. The basis
-- column is what keeps the two populations distinguishable in one table.
--
-- Ties resolve to the earliest year, so the answer does not depend on
-- evaluation order. This branch is reachable only because value is DECIMAL -
-- summed as DOUBLE, two genuinely equal years differ in the last bits and the
-- tie-break never fires.
--
-- n_periods is reported rather than filtered on. A partial year - 2026 has
-- only the quarters published so far - still competes, because the question
-- asks for the largest sum across quarters and that is the plain reading.
-- Exposing the count lets a reader see it; silently requiring four quarters
-- would change the published answers.
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.q2_best_year_by_series
(
  CONSTRAINT label_resolved EXPECT (series_label IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT one_row_per_series EXPECT (rows_for_series = 1) ON VIOLATION FAIL UPDATE,
  CONSTRAINT plausible_period_count EXPECT (n_periods BETWEEN 1 AND 4) ON VIOLATION FAIL UPDATE
)
COMMENT 'Q2. For each series, the year whose values sum highest. basis=quarterly sums Q01-Q04; basis=annual_q05 uses the annual average for the 45 series that publish no quarterly data. Ties resolve to the earliest year. Sums compare only within a units value: four quarters of an index total ~400, four quarters of a percent change ~14.'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH current_obs AS (
  SELECT series_id, year, period, value
  FROM ${catalog}.${silver_schema}.pr_observations
  WHERE __END_AT IS NULL
),

-- Which periods a series is scored on is a property of the series, decided
-- once here, rather than a filter repeated at each use.
series_basis AS (
  SELECT
    series_id,
    CASE
      WHEN max(CASE WHEN period <> 'Q05' THEN 1 ELSE 0 END) = 1 THEN 'quarterly'
      ELSE 'annual_q05'
    END AS basis
  FROM current_obs
  GROUP BY series_id
),

scoped AS (
  SELECT o.series_id, b.basis, o.year, o.value
  FROM current_obs o
  JOIN series_basis b ON b.series_id = o.series_id
  WHERE (b.basis = 'quarterly'  AND o.period <> 'Q05')
     OR (b.basis = 'annual_q05' AND o.period  = 'Q05')
),

per_year AS (
  SELECT
    series_id,
    basis,
    year,
    sum(value) AS total_value,
    count(*)   AS n_periods
  FROM scoped
  GROUP BY series_id, basis, year
),

ranked AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY series_id
      ORDER BY total_value DESC, year ASC
    ) AS rn
  FROM per_year
)

SELECT
  r.series_id,
  s.series_label,
  s.sector,
  s.measure,
  s.units,
  r.basis,
  r.year        AS best_year,
  r.total_value,
  r.n_periods,
  -- Evaluated after the WHERE, so this counts surviving rows: it is 1 unless
  -- the join to the series dimension fanned out, which would otherwise double
  -- a series in the answer with no other symptom.
  count(*) OVER (PARTITION BY r.series_id) AS rows_for_series
FROM ranked r
-- LEFT, not INNER, for the same reason as in silver.series: an INNER join
-- would drop a series whose label stopped resolving, and a missing series is
-- indistinguishable from one that never existed. The constraint above turns
-- the same event into a halt.
LEFT JOIN ${catalog}.${silver_schema}.series s
  ON s.series_id = r.series_id
WHERE r.rn = 1;


-- Q3: one series, one period, with the population for that year where one
-- exists.
--
-- The series and period come from the pipeline's configuration block rather
-- than being written here, so the report is parameterised rather than one-off.
--
-- The join is LEFT and every BLS year is kept. Population is available for 11
-- of them and null for the rest: the series begins in 1987 and runs to 2026,
-- while ACS 1-year estimates start in 2013, skip 2020 entirely - the Census
-- Bureau suspended them for the pandemic year - and currently end in 2024.
-- An INNER join would answer a different question, silently: it would report
-- only the overlap and give no sign that four decades of observations had been
-- dropped. "Where available" means the years without a population are part of
-- the answer.
--
-- The filter on the population side sits in the ON clause, not the WHERE. In
-- the WHERE it would discard the unmatched rows after the join and quietly
-- turn this back into an inner join.
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.${gold_schema}.q3_series_value_and_population
(
  CONSTRAINT one_row_per_year EXPECT (rows_for_year = 1) ON VIOLATION FAIL UPDATE,
  CONSTRAINT value_present EXPECT (value IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Q3. Value per year for the configured series and period, with the US population for that year where ACS published one. Population is null before 2013, for 2020, and after 2024 - those years are kept deliberately.'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  o.series_id,
  s.series_label,
  s.units,
  o.period,
  o.year,
  o.value,
  p.population,
  count(*) OVER (PARTITION BY o.year) AS rows_for_year
FROM ${catalog}.${silver_schema}.pr_observations o
LEFT JOIN ${catalog}.${silver_schema}.series s
  ON s.series_id = o.series_id
LEFT JOIN ${catalog}.${silver_schema}.population p
  ON p.year = o.year
 AND p.__END_AT IS NULL
WHERE o.__END_AT IS NULL
  AND o.series_id = '${q3_series_id}'
  AND o.period = '${q3_period}';
