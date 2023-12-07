import logging
import subprocess

from google.cloud import storage
import urllib

_logger = logging.getLogger(__name__)


def parse_blob_uri(uri: str, client=storage.Client()) -> storage.Blob:
    if not uri.startswith("gs://"):
        raise ValueError("Must be a fully qualified URI beginning with gs://")
    proto, *rest = uri.split("://")
    bucket, *path_segments = rest[0].split("/")
    return storage.Blob(
        name="/".join(path_segments), bucket=storage.Bucket(client=client, name=bucket)
    )


def copy_file_to_bucket(
    local_file_uri: str, remote_blob_uri: str, client=storage.Client()
):
    """
    Upload the contents of file `local_file_uri` on local filesystem, to `remote_blob_uri` in
    """
    _logger.info(f"Uploading {local_file_uri} to {remote_blob_uri}")
    blob = parse_blob_uri(remote_blob_uri)
    blob.upload_from_filename(client=client, filename=local_file_uri)
    _logger.info(f"Finished uploading {local_file_uri} to {remote_blob_uri}")


def blob_writer(blob_uri: str, client=storage.Client(), binary=True) -> storage.Blob:
    """
    Returns a file-like object that can be used to write to the blob at `blob_uri`
    """
    blob = parse_blob_uri(blob_uri, client=client)
    return blob.open("wb" if binary else "w")


def http_upload_shell(http_uri: str, blob_uri: str):
    """
    Upload the contents of `http_uri` to `blob_uri` using curl and gsutil
    """
    p = subprocess.run(
        ["bash", "-c", f"curl {http_uri} | gcloud storage cp - {blob_uri}"],
        capture_output=True,
        check=True,
    )

    if p.returncode != 0:
        raise RuntimeError(f"curl failed:\n{p.stdout}\n{p.stderr}")


def http_upload_urllib(
    http_uri: str, blob_uri: str, client=storage.Client(), chunk_size=1024 * 16
):
    _logger.info(f"Uploading {http_uri} to {blob_uri}")
    with urllib.request.urlopen(http_uri) as f:
        with blob_writer(
            blob_uri=blob_uri,
            client=client,
        ) as f_out:
            chunk = f.read(chunk_size)
            while chunk:
                f_out.write(chunk)
                chunk = f.read(chunk_size)
