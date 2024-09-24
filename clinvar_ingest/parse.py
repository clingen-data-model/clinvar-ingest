import gzip
import json
import logging
import os
import pathlib
from typing import IO, Any, Callable, Iterator, TextIO

from clinvar_ingest.cloud.gcs import blob_reader, blob_size, blob_writer
from clinvar_ingest.fs import BinaryOpenMode, ReadCounter, fs_open
from clinvar_ingest.model.common import Model, dictify
from clinvar_ingest.reader import (
    get_clinvar_rcv_xml_releaseinfo,
    get_clinvar_vcv_xml_releaseinfo,
    read_clinvar_rcv_xml,
    read_clinvar_somatic_model_vcv_xml,
    read_clinvar_vcv_xml,
)
from clinvar_ingest.utils import ClinVarIngestFileFormat, make_progress_logger

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


def clean_list(input_list: list) -> list | None:
    output = []
    for item in input_list:
        if isinstance(item, dict):
            val = clean_dict(item)
            if val is not None:
                output.append(val)
        elif isinstance(item, list):
            val = clean_list(item)
            if val is not None:
                output.append(val)
        else:
            if item not in [None, ""]:
                output.append(item)
    return output if output != [] else None


def clean_dict(input_dict: dict) -> dict | None:
    output = {}
    for k, v in input_dict.items():
        if isinstance(v, dict):
            val = clean_dict(v)
            if val is not None:
                output[k] = val
        elif isinstance(v, list):
            val = clean_list(v)
            if val is not None:
                output[k] = val
        else:
            if v is not None and len(v) > 0:
                output[k] = v
    return output if output != {} else None


def clean_object(obj: list | dict | str | None) -> dict | list | str | None:
    if isinstance(obj, dict):
        cleaned = clean_dict(obj)
        return cleaned if cleaned is not None else None
    elif isinstance(obj, list):
        cleaned = clean_list(obj)
        return cleaned if cleaned is not None else None
    else:
        return obj if obj not in [None, ""] else None


def _jsonify_non_empties(obj: list | dict | str) -> dict | list | str | None:
    """
    Jsonify objects and lists of objects, but not if it's None, empty string, or an empty collection
    """
    if isinstance(obj, dict):
        cleaned = clean_object(obj)
        return json.dumps(cleaned) if cleaned is not None else None
    elif isinstance(obj, list):
        output = []
        for o in obj:
            cleaned = clean_object(o)
            if cleaned is not None:
                output.append(json.dumps(cleaned))
        return output
    else:
        return json.dumps(obj) if obj not in [None, ""] else None


def reader_fn_for_format(
    file_format: ClinVarIngestFileFormat,
) -> Callable[[TextIO, bool], Iterator[Model]]:
    match file_format:
        case ClinVarIngestFileFormat.VCV:
            reader_fn = read_clinvar_vcv_xml
        case ClinVarIngestFileFormat.RCV:
            reader_fn = read_clinvar_rcv_xml
        case ClinVarIngestFileFormat.VCV_SOMATIC:
            reader_fn = read_clinvar_somatic_model_vcv_xml
        case _:
            raise ValueError(f"Unknown file format: {file_format}")
    return reader_fn


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
        interval=60,
    )
    object_log_progress = make_progress_logger(
        logger=_logger,
        fmt=(
            "Read {elapsed_value} "
            + iterate_type
            + " in {elapsed:.2f}s. Total: {current_value}."
        ),
        interval=60,
    )

    reader_fn = reader_fn_for_format(file_format)
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
