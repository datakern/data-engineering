-- GOLD: two small tables that answer questions. Silver keeps every kind of event
-- (earthquake, quarry blast, explosion ...). Gold decides what counts: earthquakes only.

-- "How active was each day?"
CREATE OR REFRESH MATERIALIZED VIEW quakes_daily
COMMENT 'Earthquakes per day: how many, how strong'
AS
SELECT
  event_date,
  COUNT(*)                     AS quakes,
  ROUND(AVG(magnitude), 2)     AS avg_magnitude,
  MAX(magnitude)               AS strongest_magnitude,
  COUNT_IF(magnitude >= 4)     AS quakes_m4_plus
FROM quakes_silver
WHERE event_type = 'earthquake'
GROUP BY event_date;


-- "Where is it happening?"
CREATE OR REFRESH MATERIALIZED VIEW quakes_by_region
COMMENT 'Earthquakes per region (the last part of the USGS place text)'
AS
SELECT
  region,
  COUNT(*)                     AS quakes,
  MAX(magnitude)               AS strongest_magnitude,
  ROUND(AVG(depth_km), 1)      AS avg_depth_km,
  COUNT_IF(magnitude >= 4)     AS quakes_m4_plus
FROM quakes_silver
WHERE event_type = 'earthquake'
GROUP BY region;
