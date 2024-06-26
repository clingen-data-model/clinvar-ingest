import logging

from google.cloud import storage

from clinvar_ingest.cloud.gcs import http_download_curl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d - %(levelname)s - %(name)s - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("copy_testing.ipynb")


# This is the url for a ClinVarVariationRelease xml file:
file_baseurl = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/VCV_xml_old_format/"
file_name = "ClinVarVariationRelease_2024-02.xml.gz"
file_url = file_baseurl + file_name

local_file = "./ClinVarVariationRelease_2024-02.xml.gz"

expected_file_size = 3298023159

storage_client = storage.Client()

bucket_path = "kyle-test"

# http_upload(
#     http_uri=file_url,
#     blob_uri=f"gs://clinvar-ingest/{bucket_path}/{file_name}",
#     file_size=expected_file_size,
#     client=storage_client,
#     chunk_size=8 * 1024 * 1024,
# )

http_download_curl(
    http_uri=file_url, local_path=local_file, file_size=expected_file_size
)

# http_download_requests(
#     http_uri=file_url, local_path=local_file, file_size=expected_file_size
# )
