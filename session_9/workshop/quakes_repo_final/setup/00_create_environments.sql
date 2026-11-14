-- Run once, in the Databricks SQL editor, before the first deploy.
-- Replace <catalog> and <name> with your own values (lowercase letters, digits, underscore).
--
-- An environment, in this workshop, is a schema with a volume (a folder for files) inside it.
-- dev and prod are two schemas in the same catalog.

CREATE SCHEMA IF NOT EXISTS <catalog>.<name>_dev;
CREATE VOLUME IF NOT EXISTS <catalog>.<name>_dev.landing;

CREATE SCHEMA IF NOT EXISTS <catalog>.<name>_prod;
CREATE VOLUME IF NOT EXISTS <catalog>.<name>_prod.landing;
