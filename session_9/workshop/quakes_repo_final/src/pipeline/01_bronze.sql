-- BRONZE: the files exactly as they arrived.
--
-- Every column is read as text on purpose. Bronze never rejects a row because of a surprise
-- (a missing magnitude, a strange date). Silver is where we decide what is acceptable.
--
-- A STREAMING table only reads files it has not seen before, so each run adds just the new files.
-- ${quakes.landing_path} is a pipeline setting, so dev and prod can read from their own volume.

CREATE OR REFRESH STREAMING TABLE quakes_bronze
COMMENT 'USGS earthquake events, raw text, one row per event per downloaded file'
AS
SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '${quakes.landing_path}',
  format => 'csv',
  header => true,
  schema => 'time STRING, latitude STRING, longitude STRING, depth STRING, mag STRING, magType STRING, nst STRING, gap STRING, dmin STRING, rms STRING, net STRING, id STRING, updated STRING, place STRING, type STRING, horizontalError STRING, depthError STRING, magError STRING, magNst STRING, status STRING, locationSource STRING, magSource STRING'
);
