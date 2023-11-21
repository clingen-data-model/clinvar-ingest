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

-- unspecified column modes default to NULLABLE

create or replace external table `{project}.{dataset}.variation` (
  id string,
  name string,
  variation_type string,
  subclass_type string,
  allele_id string,
  protein_change array<string>,
  num_chromosomes string,
  num_copies string,
  content string,
  child_ids array<string>,
  descendant_ids array<string>,

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
  date_created string,
  record_status string,
  species string,
  review_status string,
  interp_description string,
  num_submitters string,
  num_submissions string,
  date_last_updated string,
  interp_type string,
  interp_explanation string,
  interp_date_last_evaluated string,
  interp_content string,
  content string,
  variation_id string,
  entity_type string
)
options (
  format = "json",
  uris = [
    "gs://{bucket}{path}/variation_archive/*"
  ]
);
