# quakes: an earthquake pipeline on Databricks

Downloads the public USGS earthquake catalog every day and builds three layers:

```
USGS web API --> landing volume (CSV files) --> quakes_bronze --> quakes_silver --> quakes_daily
                                                                                \-> quakes_by_region
```

| Layer | What it holds |
|---|---|
| `quakes_bronze` | the files exactly as they arrived, all text |
| `quakes_silver` | one clean row per event, real types, bad rows dropped, newest version kept |
| `quakes_daily`, `quakes_by_region` | earthquakes only, counted per day and per region |

## What is in this repo

```
databricks.yml                  the bundle: variables and the targets (dev, prod)
resources/quakes.pipeline.yml   the pipeline, as code
resources/quakes.job.yml        the job, as code: download -> pipeline -> verify
setup/00_create_environments.sql  run once in the SQL editor: schemas and landing volumes
src/01_download_quakes.py       job task 1: download from USGS into the volume
src/pipeline/*.sql              the pipeline: bronze, silver, gold
src/99_verify.py                job task 3: checks on the finished tables
```

## Deploy

Install the Databricks CLI, log in once (`databricks auth login --host <workspace url>`), then:

```
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run quakes_job -t dev

databricks bundle deploy   -t prod      # the next environment, same files
databricks bundle run quakes_job -t prod
```

Before the first deploy, replace the `<...>` values in `databricks.yml`.

## Data source

USGS Earthquake Catalog, FDSN event web service: <https://earthquake.usgs.gov/fdsnws/event/1/>
Public, no account, no key. USGS data is in the public domain.
