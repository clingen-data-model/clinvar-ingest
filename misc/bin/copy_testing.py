import logging
from typing import Iterator
import urllib.request
import requests

from google.cloud import storage

from clinvar_ingest.cloud.gcs import blob_writer, http_upload_urllib

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

http_upload_urllib(
    http_uri=file_url,
    blob_uri=f"gs://clinvar-ingest/{bucket_path}/{file_name}",
    file_size=expected_file_size,
    client=storage_client,
    chunk_size=8 * 1024 * 1024,
)
