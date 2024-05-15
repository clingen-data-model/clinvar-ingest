import gzip
import json
import logging
import os
import pathlib
from typing import IO, Any, TextIO

from clinvar_ingest.cloud.gcs import blob_reader, blob_size, blob_writer
from clinvar_ingest.fs import BinaryOpenMode, ReadCounter, fs_open
from clinvar_ingest.model.common import dictify
from clinvar_ingest.reader import get_clinvar_xml_releaseinfo, read_clinvar_xml
from clinvar_ingest.utils import make_progress_logger

_logger = logging.getLogger("clinvar_ingest")

GZIP_COMPRESSLEVEL = int(os.environ.get("GZIP_COMPRESSLEVEL", 9))


def _st_size(filepath: str):
    if filepath.startswith("gs://"):
        return blob_size(filepath)
    else:
        return pathlib.Path(filepath).stat().st_size


def _open(
    filepath: str, mode: BinaryOpenMode = BinaryOpenMode.READ
) -> ReadCounter | TextIO | IO[Any] | gzip.GzipFile:
    _logger.debug(f"Opening file: {filepath}, mode: {mode}")
    if filepath.startswith("gs://"):
        if mode == BinaryOpenMode.WRITE:
            f = blob_writer(filepath)
        elif mode == BinaryOpenMode.READ:
            f = blob_reader(filepath)
        else:
            raise ValueError(f"Unknown mode: {mode}")

        if filepath.endswith(".gz"):
            # wraps BlobReader in gzip.GzipFile, which implements .tell()
            return gzip.open(f, mode=str(mode), compresslevel=GZIP_COMPRESSLEVEL)  # type: ignore
        else:
            # Need to wrap in a counter so we can track bytes read
            return ReadCounter(f)
    else:
        return fs_open(filepath, mode=mode, make_parents=True)


def get_open_file_for_writing(
    d: dict,
    root_dir: str,
    label: str,
    suffix=".ndjson",
):
    """
    Takes a dictionary of labels to file handles. Opens a new file handle using
    label and suffix in root_dir if not already in the dictionary.

    Adds a _name attribute for the path opened.
    """
    if label not in d:
        label_dir = f"{root_dir}/{label}"
        filepath = f"{label_dir}/{label}{suffix}"
        _logger.info("Opening file for writing: %s", filepath)
        d[label] = _open(filepath, mode=BinaryOpenMode.WRITE)
        setattr(d[label], "_name", filepath)
    return d[label]


def parse_and_write_files(
    input_filename: str,
    output_directory: str,
    gzip_output=True,
    disassemble=True,
    jsonify_content=True,
) -> dict[str, str]:
    """
    Parses input file, writes outputs to output directory.

    Returns the dict of types to their output files.
    """
    open_output_files = {}
    with _open(input_filename) as f_in:  # type: ignore
        releaseinfo = get_clinvar_xml_releaseinfo(f_in)
        release_date = releaseinfo["release_date"]
        _logger.debug(f"Parsing release date: {release_date}")

    # Release directory is within the output directory
    output_release_directory = f"{output_directory}/{release_date}"

    # input_file_size = _st_size(input_filename)
    vcv_count = 0
    byte_log_progress = make_progress_logger(
        logger=_logger,
        fmt="Read {elapsed_value} bytes in {elapsed:.2f}s. Total bytes read: {current_value}.",
    )
    vcv_log_progress = make_progress_logger(
        logger=_logger,
        fmt="Read {elapsed_value} VariationArchives in {elapsed:.2f}s. Total VariationArchives read: {current_value}.",
    )

    try:
        with _open(input_filename) as f_in:  # type: ignore
            byte_log_progress(0)  # initialize
            vcv_log_progress(0)  # initialize

            for obj in read_clinvar_xml(f_in, disassemble=disassemble):
                entity_type = obj.entity_type
                f_out = get_open_file_for_writing(
                    open_output_files,
                    root_dir=output_release_directory,
                    label=entity_type,
                    suffix=".ndjson" if not gzip_output else ".ndjson.gz",
                )
                obj_dict = dictify(obj)
                assert isinstance(obj_dict, dict), obj_dict

                # jsonify content type fields if requested
                if jsonify_content:
                    if hasattr(type(obj), "jsonifiable_fields"):
                        for field in getattr(type(obj), "jsonifiable_fields")():
                            if field in obj_dict:
                                if isinstance(obj_dict[field], list):
                                    obj_dict[field] = [
                                        json.dumps(i) for i in obj_dict[field]
                                    ]
                                else:
                                    obj_dict[field] = json.dumps(obj_dict[field])

                obj_dict["release_date"] = release_date
                f_out.write(json.dumps(obj_dict).encode("utf-8"))
                f_out.write("\n".encode("utf-8"))

                # Log offset and count for monitoring
                byte_log_progress(f_in.tell())
                if entity_type == "variation_archive":
                    vcv_count += 1
                    vcv_log_progress(vcv_count)

            # Log final status
            byte_log_progress(f_in.tell(), force=True)
            vcv_log_progress(vcv_count, force=True)

    except Exception as e:
        _logger.critical("Exception caught in parse_and_write_files")
        raise e
    finally:
        _logger.debug("Closing output files")
        for f in open_output_files.values():
            f.close()

    return {k: v._name for k, v in open_output_files.items()}
