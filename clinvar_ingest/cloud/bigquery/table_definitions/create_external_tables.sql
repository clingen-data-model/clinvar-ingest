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
  release_date date,
  name string,
  variation_type string,
  subclass_type string,
  allele_id string,
  protein_change array<string>,
  num_chromosomes integer,
  num_copies integer,
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
  release_date date,
  name string,
  version string,
  date_created date,
  record_status string,
  species string,
  review_status string,
  interp_description string,
  num_submitters integer,
  num_submissions integer,
  date_last_updated date,
  interp_type string,
  interp_explanation string,
  interp_date_last_evaluated date,
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

create or replace external table `{project}.{dataset}.gene` (
  id string,
  release_date date,
  hgnc_id string,
  symbol string,
  full_name string,
  entity_type string
)
options (
  format = "json",
  uris = [
    "gs://{bucket}{path}/gene/*"
  ]
);

create or replace external table `{project}.{dataset}.gene_association` (
  release_date date,
  variation_id string,
  gene_id string,
  relationship_type string,
  source string,
  content string,
  entity_type string
)
options (
  format = "json",
  uris = [
    "gs://{bucket}{path}/gene_association/*"
  ]
);
