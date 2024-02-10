import logging
import time
import urllib

from google.cloud import storage

_logger = logging.getLogger(__name__)


def parse_blob_uri(uri: str, client: storage.Client = None) -> storage.Blob:
    if not uri.startswith("gs://"):
        raise ValueError("Must be a fully qualified URI beginning with gs://")
    if client is None:
        client = storage.Client()
    proto, *rest = uri.split("://")
    bucket, *path_segments = rest[0].split("/")
    return storage.Blob(
        name="/".join(path_segments), bucket=storage.Bucket(client=client, name=bucket)
    )


def copy_file_to_bucket(
    local_file_uri: str, remote_blob_uri: str, client: storage.Client = None
):
    """
    Upload the contents of file `local_file_uri` on local filesystem, to `remote_blob_uri` in
    """
    _logger.info(f"Uploading {local_file_uri} to {remote_blob_uri}")
    if client is None:
        client = storage.Client()
    blob = parse_blob_uri(remote_blob_uri, client=client)
    blob.upload_from_filename(client=client, filename=local_file_uri)
    _logger.info(f"Finished uploading {local_file_uri} to {remote_blob_uri}")


def blob_writer(
    blob_uri: str, client: storage.Client = None, binary=True
) -> storage.Blob:
    """
    Returns a file-like object that can be used to write to the blob at `blob_uri`
    """
    if client is None:
        client = storage.Client()
    blob = parse_blob_uri(blob_uri, client=client)
    return blob.open("wb" if binary else "w")


def blob_reader(
    blob_uri: str, client: storage.Client = None, binary=True
) -> storage.Blob:
    """
    Returns a file-like object that can be used to read from the blob at `blob_uri`
    """
    if client is None:
        client = storage.Client()
    blob = parse_blob_uri(blob_uri, client=client)
    return blob.open("rb" if binary else "r")


def http_upload_urllib(
    http_uri: str,
    blob_uri: str,
    file_size: int,
    client: storage.Client = None,
    chunk_size=8 * 1024 * 1024,
):
    """
    Upload the contents of `http_uri` to `blob_uri` using urllib.request.urlopen and Blob.open
    """
    _logger.info(f"Uploading {http_uri} to {blob_uri}")
    if client is None:
        client = storage.Client()

    bytes_read = 0
    with urllib.request.urlopen(http_uri) as f:
        opened_file_size = int(f.headers.get("Content-Length"))
        if opened_file_size != file_size:
            raise RuntimeError(
                f"File size mismatch. Expected {file_size} but got {opened_file_size}."
            )
        with blob_writer(blob_uri=blob_uri, client=client) as f_out:
            while bytes_read < file_size:
                chunk = f.read(chunk_size)
                chunk_bytes_read = len(chunk)
                bytes_read += chunk_bytes_read
                _logger.info(
                    f"Read {chunk_bytes_read} bytes from {http_uri}. Total bytes read: {bytes_read}."
                )

                if len(chunk) > 0:
                    f_out.write(chunk)

                if len(chunk) == 0:
                    wait_time = 10
                    _logger.warning(
                        f"Received an empty chunk from {http_uri} at byte {bytes_read}. Pausing {wait_time} seconds"
                    )
                    time.sleep(wait_time)

    # Sanity check for bytes read == file_size
    if bytes_read != file_size:
        raise RuntimeError(
            f"Upload of {http_uri} to {blob_uri} failed. Read {bytes_read} of {file_size}."
        )
