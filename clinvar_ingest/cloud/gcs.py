import logging
import queue
import subprocess
import threading
import time
from pathlib import Path, PurePath

import requests
from google.cloud import storage

_logger = logging.getLogger("clinvar_ingest")


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


def http_upload(
    http_uri: str,
    blob_uri: str,
    file_size: int,
    client: storage.Client = None,
    chunk_size=8 * 1024 * 1024,
):
    """
    Upload the contents of `http_uri` to `blob_uri` using requests.get and Blob.open
    """
    _logger.info(f"Uploading {http_uri} to {blob_uri}")
    if client is None:
        client = storage.Client()

    bytes_read = 0
    response = requests.get(http_uri, stream=True, timeout=10)
    response.raise_for_status()
    opened_file_size = int(response.headers.get("Content-Length"))
    if opened_file_size != file_size:
        raise RuntimeError(
            f"File size mismatch. Expected {file_size} but got {opened_file_size}."
        )
    with blob_writer(blob_uri=blob_uri, client=client) as f_out:
        for chunk in response.iter_content(chunk_size=chunk_size):
            chunk_bytes_read = len(chunk)
            bytes_read += chunk_bytes_read
            _logger.info(
                f"Read {chunk_bytes_read} bytes from {http_uri}. Total bytes read: {bytes_read}/{opened_file_size}."
            )

            if len(chunk) > 0:
                f_out.write(chunk)

            if len(chunk) == 0:
                wait_time = 10
                _logger.warning(
                    f"Received an empty chunk from {http_uri} at byte {bytes_read}. Pausing {wait_time} seconds"
                )
                time.sleep(wait_time)


def http_download_requests(
    http_uri: str,
    local_path: PurePath,
    file_size: int,
    chunk_size=8 * 1024 * 1024,
):
    """
    Upload the contents of `http_uri` to `blob_uri` using requests.get and Blob.open
    """
    _logger.info(f"Downloading {http_uri} to {local_path}")

    bytes_read = 0
    response = requests.get(http_uri, stream=True, timeout=10)
    response.raise_for_status()
    opened_file_size = int(response.headers.get("Content-Length"))

    def log_progress():
        if getattr(log_progress, "prev_log_time", None) is None:
            log_progress.prev_log_time = time.time()
            log_progress.prev_log_bytes = 0
            return
        now = time.time()
        if now - log_progress.prev_log_time > 5:
            elapsed = now - log_progress.prev_log_time
            elapsed_bytes = bytes_read - log_progress.prev_log_bytes
            _logger.info(
                f"Read {elapsed_bytes} bytes in {elapsed:.2f} seconds. Total bytes read: {bytes_read}/{opened_file_size}."
            )
            log_progress.prev_log_time = now
            log_progress.prev_log_bytes = bytes_read

    log_progress()  # initialize

    if opened_file_size != file_size:
        raise RuntimeError(
            f"File size mismatch. Expected {file_size} but got {opened_file_size}."
        )
    with open(local_path, "wb") as f_out:
        for chunk in response.iter_content(chunk_size=chunk_size):
            chunk_bytes_read = len(chunk)
            bytes_read += chunk_bytes_read

            log_progress()

            if len(chunk) > 0:
                f_out.write(chunk)

            if len(chunk) == 0:
                wait_time = 10
                _logger.warning(
                    f"Received an empty chunk from {http_uri} at byte {bytes_read}. Pausing {wait_time} seconds"
                )
                time.sleep(wait_time)

    return Path(local_path)


def http_download_curl(
    http_uri: str,
    local_path: PurePath,
    file_size: int,
    # client: storage.Client = None,
    # chunk_size=8 * 1024 * 1024,
) -> Path:
    p = subprocess.Popen(
        ["curl", "-o", local_path, http_uri],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    def reader(pipe: subprocess.PIPE, q: queue.Queue):
        try:
            with pipe:
                for line in iter(pipe.readline, b""):
                    q.put((pipe, line))
        finally:
            q.put(None)

    q = queue.Queue()
    t1 = threading.Thread(target=reader, args=(p.stdout, q))
    t1.start()
    t2 = threading.Thread(target=reader, args=(p.stderr, q))
    t2.start()

    def file_stat(path: Path, q: queue.Queue):
        while True:
            try:
                _ = q.get_nowait()
                break
            except queue.Empty:
                if not path.exists():
                    _logger.info(f"{path} does not exist")
                else:
                    _logger.info(f"{path} size: {path.stat().st_size}")
            time.sleep(10)

    t_stat_stop = queue.Queue()
    t_stat = threading.Thread(target=file_stat, args=(Path(local_path), t_stat_stop))
    t_stat.start()

    for _ in range(2):
        for pipe, line in iter(q.get, None):
            _logger.info(f"{pipe}: {line.decode('utf-8')}")

    returncode = p.wait()
    _logger.info(f"curl return code: {returncode}")

    t_stat_stop.put(None)

    actual_path = Path(local_path)
    if not actual_path.exists():
        raise RuntimeError(f"File {local_path} does not exist")
    if actual_path.stat().st_size != file_size:
        raise RuntimeError(
            f"File size mismatch. Expected {file_size} but got {actual_path.stat().st_size}."
        )
    return actual_path
