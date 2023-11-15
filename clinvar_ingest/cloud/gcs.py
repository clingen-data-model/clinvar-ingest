import logging

from google.cloud import storage

_logger = logging.getLogger(__name__)

client = storage.Client()


def parse_blob_uri(uri: str) -> storage.Blob:
    if not uri.startswith("gs://"):
        raise ValueError("Must be a fully qualified URI beginning with gs://")
    proto, *rest = uri.split("://")
    bucket, *path_segments = rest[0].split("/")
    return storage.Blob(
        name="/".join(path_segments), bucket=storage.Bucket(client=client, name=bucket)
    )


def copy_file_to_bucket(local_file_uri: str, remote_blob_uri):
    """
    Upload the contents of file `local_file_uri` on local filesystem, to `remote_blob_uri` in
    """
    _logger.info(f"Uploading {local_file_uri} to {remote_blob_uri}")
    blob = parse_blob_uri(remote_blob_uri)
    blob.upload_from_filename(client=client, filename=local_file_uri)
    _logger.info(f"Finished uploading {local_file_uri} to {remote_blob_uri}")
