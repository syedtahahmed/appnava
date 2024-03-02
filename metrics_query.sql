-- Calculate Average Daily Duration
WITH user_daily_duration AS (
  SELECT
    user_id,
    COUNT(DISTINCT DATE(TIMESTAMP_SECONDS(CAST(event_timestamp / 1000000 AS INT64)))) AS active_days,
    CAST(SUM(EXTRACT(SECOND FROM TIMESTAMP_SECONDS(CAST(event_timestamp / 1000000 AS INT64)) - TIMESTAMP_SECONDS(CAST(event_previous_timestamp / 1000000 AS INT64)))) AS INT64) AS total_duration_seconds
  FROM
    `appnava-project.appnava.data1`
  WHERE
    event_name = 'session_start' -- Consider only session start events
    AND event_previous_timestamp IS NOT NULL -- Exclude sessions with null previous timestamps
  GROUP BY
    user_id
)

SELECT
  user_id,
  total_duration_seconds / active_days AS avg_daily_duration_seconds
FROM
  user_daily_duration;

-- Calculate Average Session Duration
WITH session_duration AS (
  SELECT
    user_id,
    CAST(AVG(EXTRACT(SECOND FROM TIMESTAMP_SECONDS(CAST(event_timestamp / 1000000 AS INT64)) - TIMESTAMP_SECONDS(CAST(event_previous_timestamp / 1000000 AS INT64)))) AS INT64) AS avg_session_duration_seconds
  FROM
    `appnava-project.appnava.data1`
  WHERE
    event_name = 'session_start' -- Consider only session start events
    AND event_previous_timestamp IS NOT NULL -- Exclude sessions with null previous timestamps
  GROUP BY
    user_id
)

SELECT
  user_id,
  avg_session_duration_seconds
FROM
  session_duration;

-- Calculate Engaged Sessions per User
WITH engaged_sessions AS (
  SELECT
    user_id,
    COUNT(*) AS total_sessions,
    COUNT(DISTINCT DATE(TIMESTAMP_SECONDS(CAST(event_timestamp / 1000000 AS INT64)))) AS active_days
  FROM
    `appnava-project.appnava.data1`
  WHERE
    event_name = 'session_start' -- Consider only session start events
    AND event_previous_timestamp IS NOT NULL -- Exclude sessions with null previous timestamps
  GROUP BY
    user_id
)

SELECT
  user_id,
  total_sessions / active_days AS engaged_sessions_per_user
FROM
  engaged_sessions;
