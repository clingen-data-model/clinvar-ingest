import json
import re

import google.cloud.storage as storage

# e.g.
# gs://clinvar-ingest-dev/prior_xml_archives/vcv/ClinVarVCVRelease_2024-0126.xml.gz
bucket = "clinvar-ingest-dev"
prefix = "prior_xml_archives/vcv/"

output_file = "prior_xml_archives-ftp-watcher.txt"

gs_client = storage.Client()


def filename_to_release_date(filename: str) -> str | None:
    """
    Extract the release date from a filename.
    """
    # ClinVarVCVRelease_2024-0126.xml.gz
    # date_str = filename.split("_")[-1].split(".")[0]
    pattern = re.compile(r".*(\d{4})-(\d{2})(\d{2}).xml.gz")
    if match := pattern.match(str(filename)):
        year, month, day = match.groups()
        return f"{year}-{month}-{day}"
    return None


# List a bucket's objects with a prefix
def list_prefix(bucket, prefix) -> list[str]:
    """
    List all objects in a bucket with a given prefix.
    """
    blobs = gs_client.list_blobs(bucket, prefix=prefix)
    return [blob.name for blob in blobs]


# e.g.
# print(list_prefix(bucket, prefix))
# ['prior_xml_archives/vcv/', 'prior_xml_archives/vcv/ClinVarVCVRelease_2024-0126.xml.gz', 'prior_xml_archives/vcv/ClinVarVCVRelease_2024-0205.xml.gz', 'prior_xml_archives/vcv/ClinVarVCVRelease_2024-0214.xml.gz', 'prior_xml_archives/vcv/ClinVarVCVRelease_2024-0221.xml.gz', 'prior_xml_archives/vcv/ClinVarVCVRelease_2024-0229.xml.gz', 'prior_xml_archives/vcv/ClinVarVCVRelease_2024-0306.xml.gz', 'prior_xml_archives/vcv/ClinVarVCVRelease_2024-0311.xml.gz', 'prior_xml_archives/vcv/ClinVarVCVRelease_2024-0331.xml.gz', 'prior_xml_archives/vcv/ClinVarVCVRelease_2024-0502.xml.gz', 'prior_xml_archives/vcv/ClinVarVCVRelease_2024-0603.xml.gz', 'prior_xml_archives/vcv/ClinVarVCVRelease_2024-0611.xml.gz', 'prior_xml_archives/vcv/ClinVarVCVRelease_2024-0618.xml.gz', 'prior_xml_archives/vcv/ClinVarVCVRelease_2024-0624.xml.gz', 'prior_xml_archives/vcv/ClinVarVCVRelease_2024-0630.xml.gz', 'prior_xml_archives/vcv/ClinVarVCVRelease_2024-0708.xml.gz', 'prior_xml_archives/vcv/ClinVarVCVRelease_2024-0716.xml.gz']


# Generate a list of (release-date, blob) tuples sorted by release-date
def get_release_file_pairs(bucket, prefix) -> list[tuple[str, storage.Blob]]:
    """
    List all objects in a bucket with a given prefix, sorted by release date.
    """
    blobs = gs_client.list_blobs(bucket, prefix=prefix)
    release_date_blob = []
    for blob in blobs:
        release_date = filename_to_release_date(blob.name)
        print(f"file: {blob.name} release_date: {release_date}")
        if release_date:
            release_date_blob.append((release_date, blob))
    return sorted(release_date_blob, key=lambda date_name: date_name[0])


release_file_pairs = get_release_file_pairs(bucket, prefix)
print(release_file_pairs)
output_records = []
for release_date, blob in release_file_pairs:
    print(release_date, blob.name)

    example = [
        {
            "Name": "ClinVarRCVRelease_2024-1027.xml.gz",
            "Size": 4561188864,
            "Released": "2024-10-28 06:11:13",
            "Last Modified": "2024-10-28 06:11:13",
            "Directory": "/xml_archives/rcv",
            "Host": "gs://clinvar-ingest-dev",
            "Release Date": "2024-10-27",
        }
    ]
    if blob.size is None:
        raise ValueError(f"Blob {blob.name} size not loaded")
    blob_basename = blob.name.split("/")[-1]
    blob_prefix = "/".join(blob.name.split("/")[:-1])
    rec = [
        {
            "Name": blob.name.split("/")[-1],
            "Size": blob.size,
            # Doesn't really matter, just gonna set it to noon on the release date
            "Released": f"{release_date} 12:00:00",
            "Last Modified": f"{release_date} 12:00:00",
            "Directory": "/" + blob_prefix,
            "Host": f"gs://{blob.bucket.name}",
            "Release Date": release_date,
        }
    ]
    print(json.dumps(rec, indent=2))
    output_records.append(rec)

with open(output_file, "w") as f:
    for rec in output_records:
        f.write(json.dumps(rec) + "\n")
