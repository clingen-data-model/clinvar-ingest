-- e.g.
-- create external table `clingen-dev.clinvar_ingest_new.variation` (
--   id string,
--   name string,
--   variation_type string,
--   subclass_type string,
--   entity_type string
-- )
-- options (
--   format = "json",
--   uris = [
--     "gs://clinvar-ingest/Variation.ndjson"
--   ]
-- )


create or replace external table `{project}.{dataset}.variation` (
  id string,
  name string,
  variation_type string,
  subclass_type string,
  entity_type string
)
options (
  format = "json",
  uris = [
    "gs://{bucket}{path}/variation/*"
  ]
);

create or replace external table `{project}.{dataset}.variation_archive` (
  id string,
  name string,
  version string,
  variation string,
  entity_type string
)
options (
  format = "json",
  uris = [
    "gs://{bucket}{path}/variation_archive/*"
  ]
);
