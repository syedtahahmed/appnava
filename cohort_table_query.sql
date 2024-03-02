-- Step 1: Define cohorts by month
WITH user_cohorts AS (
  SELECT
    data.user_id,
    EXTRACT(YEAR FROM TIMESTAMP_SECONDS(CAST(data.user_first_touch_timestamp / 1000000 AS INT64))) AS cohort_year,
    EXTRACT(MONTH FROM TIMESTAMP_SECONDS(CAST(data.user_first_touch_timestamp / 1000000 AS INT64))) AS cohort_month
  FROM
    `appnava-project.appnava.data1` AS data
),

-- Step 2: Calculate the number of users returning in each cohort for day 1, day 7, and day 30
user_retention AS (
  SELECT
    cohort_year,
    cohort_month,
    COUNT(DISTINCT data.user_id) AS total_users,
    COUNTIF(EXTRACT(DAY FROM TIMESTAMP_SECONDS(CAST(data.event_timestamp / 1000000 AS INT64))) = 1) AS retained_day1,
    COUNTIF(EXTRACT(DAY FROM TIMESTAMP_SECONDS(CAST(data.event_timestamp / 1000000 AS INT64))) <= 7) AS retained_day7,
    COUNTIF(EXTRACT(DAY FROM TIMESTAMP_SECONDS(CAST(data.event_timestamp / 1000000 AS INT64))) <= 30) AS retained_day30
  FROM
    user_cohorts
  JOIN
    `appnava-project.appnava.data1` AS data ON user_cohorts.user_id = data.user_id
  GROUP BY
    cohort_year,
    cohort_month
)

-- Step 3: Calculate the retention rate for each cohort and time period
SELECT
  cohort_year,
  cohort_month,
  total_users,
  retained_day1,
  retained_day7 / total_users AS retention_rate_day7,
  retained_day30 / total_users AS retention_rate_day30
FROM
  user_retention
ORDER BY
  cohort_year DESC,
  cohort_month DESC