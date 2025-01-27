**ClinVar Ingest Process - Release Date Handling Overview** 

**Overview** 

The ClinVar ingest pipeline consists of automatically detecting and consuming
ClinVar VCV and RCV releases from the ClinVar FTP site into a single BigQuery dataset. 
ClinVar will generally release the VCV and RCV files within a day of each other, but 
most often the files are released on the same day.

The pipline has four distinct phases: 1) VCV file processing, 2) RCV file processing, 3)
BigQuery (BQ) ingestion and 4) stored procedure execution. 

**Release Date Matching**

Due to the non-deterministic nature of the timing of when VCV and RCV files appear on 
the ClinVar FTP site, the ingest processing pipeline will rely on the internal XML release dates
within the respective files for determining which files to join into the single BQ dataset.

The VCV file processing will extract the "ReleaseDate" attribute from the "ClinVarVariationRelease" 
XML element and refer to that as its release date. 

`<ClinVarVariationRelease xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://ftp.ncbi.nlm.nih.gov/pub/clinvar/xsd_public/ClinVar_VCV_2.1.xsd" ReleaseDate="2024-10-26">`

The RCV file processing will extract the "Dated"
attribute from the "ReleaseSet" XML element and use that date as its release date. 
    
`<ReleaseSet Type="full" xsi:noNamespaceSchemaLocation="http://ftp.ncbi.nlm.nih.gov/pub/clinvar/xsd_public/RCV/ClinVar_RCV_2.0.xsd" Dated="2024-10-25" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">`

These VCV and RCV file release dates may or may not be the same. 

The VCV and RCV files are copied into Google Cloud Storage (GCS) where the storage 
bucket directory names are derived from the VCV and RCV release dates respectively.
For example, a given VCV release might be named "ClinVarVCVRelease_2024-1027.xml.gz" 
but the internal release date could be "2024-10-26." The file will be stored in a 
GCS bucket with a gcs subdirectory of
"gs://<bucket_name>/execution/clinvar_vcv_2024-10-26_<release_version>" where release version
is of the semantic version form "v#\_#\_#." As each VCV and RCV file is processed a
record is saved in the "clinvar_ingest.processing_history" table containing the
internal file release date and other information about when the processing occurred.

Continuing the example above, the VCV file is stored in the GCS bucket
"gs://<bucket_name>/execution/clinvar_vcv_2024-10-26_v2_1_0/clinvar_xml/2024-10-26." The
actual JSON records that have been parsed into individual table types are stored in files in
"gs://<bucket_name>/execution/clinvar_vcv_2024-10-26_v2_1_0/clinvar_parsed/2024-10-26."

The BQ ingest portion of the pipeline determines which VCV and RCV files to pair into a
single BQ dataset. This determination is made based of the individual release dates that
are within a single day of each other. The JSON files from the paired VCV and RCV files
are ingested into external then internal BQ tables. The dataset name is determined from 
the VCV file internal release date. For example, if the VCV file has an internal release
date of "2024-10-26" and the RCV file has an internal release date of "2024-10-25", the
BQ dataset name will be "clinvar_2024_10_26_<release_version>."

Though rare, there may be occasion where the VCV and RCV files are released more than a day apart.
For those instances, the "clinvar_ingest.processing_history" table will need to be manually
updated to change the RCV release date to be one day apart from the VCV release date
so that the BQ ingest portion of the pipeline runs properly.

**Reprocessing Files**

Rare are the instances where ClinVar will re-release files in the case
where personal identifiable information (PII) is mistakenly included in a release.
In those instances, the RCV and VCV files being re-released will be stored in
a GCP bucket with "_reprocessed" appended to the name. Keeping with the example above,
our reprocessed VCV file would be stored as
"gs://<bucket_name>/execution/clinvar_vcv_2024-10-26_v2_1_0_reprocessed/clinvar_xml/2024-10-26." The
actual JSON records that have been parsed into individual table types are stored in files in
"gs://<bucket_name>/execution/clinvar_vcv_2024-10-26_v2_1_0_reprocessed/clinvar_parsed/2024-10-26."
The RCV files will be similarly placed in a storage bucket with "_reprocessed" appended.
The BQ ingest will process the reprocessed VCV and RCV files into the original dataset name.
