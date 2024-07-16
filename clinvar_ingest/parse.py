import gzip
import json
import logging
import os
import pathlib
from enum import StrEnum
from typing import IO, Any, TextIO

from clinvar_ingest.cloud.gcs import blob_reader, blob_size, blob_writer
from clinvar_ingest.fs import BinaryOpenMode, ReadCounter, fs_open
from clinvar_ingest.model.common import dictify
from clinvar_ingest.reader import (
    get_clinvar_rcv_xml_releaseinfo,
    get_clinvar_vcv_xml_releaseinfo,
    read_clinvar_rcv_xml,
    read_clinvar_vcv_xml,
)
from clinvar_ingest.utils import make_progress_logger

_logger = logging.getLogger("clinvar_ingest")

GZIP_COMPRESSLEVEL = int(os.environ.get("GZIP_COMPRESSLEVEL", 9))


class ClinVarIngestFileFormat(StrEnum):
    VCV = "vcv"
    RCV = "rcv"


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


def _jsonify_non_empties(obj) -> list | str | None:
    """
    Jsonify objects and lists of objects, but not if it's None, empty string, or an empty collection
    """
    if isinstance(obj, list):
        return [_jsonify_non_empties(i) for i in obj]
    elif obj:
        return json.dumps(obj)


def parse_and_write_files(
    input_filename: str,
    output_directory: str,
    gzip_output=True,
    disassemble=True,
    jsonify_content=True,
    file_format: ClinVarIngestFileFormat = ClinVarIngestFileFormat.VCV,
    limit: None | int = None,
) -> dict[str, str]:
    """
    Parses input file, writes outputs to output directory.

    Returns the dict of types to their output files.
    """
    open_output_files = {}
    with _open(input_filename) as f_in:  # type: ignore
        match file_format:
            case ClinVarIngestFileFormat.VCV:
                releaseinfo = get_clinvar_vcv_xml_releaseinfo(f_in)
                iterate_type = "variation_archive"
            case ClinVarIngestFileFormat.RCV:
                releaseinfo = get_clinvar_rcv_xml_releaseinfo(f_in)
                iterate_type = "rcv_mapping"
            case _:
                raise ValueError(f"Unknown file format: {file_format}")
        release_date = releaseinfo["release_date"]
        _logger.info(f"Parsing release date: {release_date}")

    # Release directory is within the output directory
    output_release_directory = f"{output_directory}/{release_date}"

    # input_file_size = _st_size(input_filename)
    object_count = 0
    byte_log_progress = make_progress_logger(
        logger=_logger,
        fmt="Read {elapsed_value} bytes in {elapsed:.2f}s. Total bytes read: {current_value}.",
    )
    object_log_progress = make_progress_logger(
        logger=_logger,
        fmt=(
            "Read {elapsed_value} "
            + iterate_type
            + " in {elapsed:.2f}s. Total VariationArchives read: {current_value}."
        ),
    )

    reader_fn = (
        read_clinvar_vcv_xml
        if file_format == ClinVarIngestFileFormat.VCV
        else read_clinvar_rcv_xml
    )
    _logger.info(f"Reading file format: {file_format} with reader: {reader_fn}")

    try:
        with _open(input_filename) as f_in:  # type: ignore
            byte_log_progress(0)  # initialize
            object_log_progress(0)  # initialize

            for obj in reader_fn(f_in, disassemble=disassemble):
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
                                obj_dict[field] = _jsonify_non_empties(obj_dict[field])

                obj_dict["release_date"] = release_date
                f_out.write(json.dumps(obj_dict).encode("utf-8"))
                f_out.write("\n".encode("utf-8"))

                # Log offset and count for monitoring
                byte_log_progress(f_in.tell())
                if entity_type == iterate_type:
                    object_count += 1
                    object_log_progress(object_count)

                if limit and object_count >= limit:
                    _logger.info("Hard limit reached: %d", limit)
                    break

            # Log final status
            byte_log_progress(f_in.tell(), force=True)
            object_log_progress(object_count, force=True)

    except Exception as e:
        _logger.critical("Exception caught in parse_and_write_files")
        raise e
    finally:
        _logger.debug("Closing output files")
        for f in open_output_files.values():
            f.close()

    table_file_pairs = {k: v._name for k, v in open_output_files.items()}
    _logger.info("Output files: %s", json.dumps(table_file_pairs))
    return table_file_pairs
