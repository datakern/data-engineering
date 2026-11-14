-- SILVER: one clean row per earthquake event, with real data types.
--
-- Step 1 (typed):  turn the text into timestamps and numbers, rename columns to plain names.
--                  try_cast gives NULL instead of an error when a value cannot be converted.
-- Step 2 (ranked): the same event can sit in several files, because USGS revises recent events.
--                  Number the versions of each event, newest first.
-- Step 3:          keep version 1 only.
--
-- The CONSTRAINT lines are data quality rules. The pipeline counts, per rule, how many rows broke it
-- (open the pipeline run and look at the data quality tab).
--   * with ON VIOLATION DROP ROW  the broken row is removed.
--   * with nothing after it       the row is kept and the violation is only counted (a warning).
--
-- Note: a magnitude below 0 is NOT an error. Very small quakes really do have negative magnitudes.

CREATE OR REFRESH MATERIALIZED VIEW quakes_silver (
  CONSTRAINT has_event_id     EXPECT (event_id IS NOT NULL)                ON VIOLATION DROP ROW,
  CONSTRAINT has_event_time   EXPECT (event_time IS NOT NULL)              ON VIOLATION DROP ROW,
  CONSTRAINT has_magnitude    EXPECT (magnitude IS NOT NULL)               ON VIOLATION DROP ROW,
  CONSTRAINT valid_latitude   EXPECT (latitude BETWEEN -90 AND 90)         ON VIOLATION DROP ROW,
  CONSTRAINT valid_longitude  EXPECT (longitude BETWEEN -180 AND 180)      ON VIOLATION DROP ROW,
  CONSTRAINT is_reviewed      EXPECT (status = 'reviewed')
)
COMMENT 'One row per earthquake event (latest version), typed and checked'
AS
WITH typed AS (
  SELECT
    id                                   AS event_id,
    try_cast(time AS TIMESTAMP)          AS event_time,
    try_cast(time AS DATE)               AS event_date,
    try_cast(latitude AS DOUBLE)         AS latitude,
    try_cast(longitude AS DOUBLE)        AS longitude,
    try_cast(depth AS DOUBLE)            AS depth_km,
    try_cast(mag AS DOUBLE)              AS magnitude,
    magType                              AS magnitude_type,
    place,
    -- "10 km WNW of The Geysers, CA" -> "CA".  A place without a comma is its own region.
    COALESCE(NULLIF(regexp_extract(place, ',\\s*([^,]+)$', 1), ''), place) AS region,
    type                                 AS event_type,
    status,
    net                                  AS network,
    try_cast(updated AS TIMESTAMP)       AS updated_at,
    ingested_at
  FROM quakes_bronze
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY event_id
      ORDER BY updated_at DESC, ingested_at DESC
    ) AS version_rank
  FROM typed
)
SELECT
  event_id, event_time, event_date, latitude, longitude, depth_km, magnitude, magnitude_type,
  place, region, event_type, status, network, updated_at
FROM ranked
WHERE version_rank = 1;
