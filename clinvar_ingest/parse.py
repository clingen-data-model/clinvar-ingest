import gzip
import json
import logging

from clinvar_ingest.cloud.gcs import blob_reader, blob_writer
from clinvar_ingest.fs import BinaryOpenMode
from clinvar_ingest.fs import _open as _fs_open
from clinvar_ingest.model import dictify
from clinvar_ingest.reader import get_clinvar_xml_releaseinfo, read_clinvar_xml

_logger = logging.getLogger("clinvar-ingest")


def _open(filepath: str, mode: BinaryOpenMode = BinaryOpenMode.READ):
    _logger.debug(f"Opening file: {filepath}, mode: {mode}")
    if filepath.startswith("gs://"):
        if mode == BinaryOpenMode.WRITE:
            f = blob_writer(filepath)
        elif mode == BinaryOpenMode.READ:
            f = blob_reader(filepath)
        else:
            raise ValueError(f"Unknown mode: {mode}")

        if filepath.endswith(".gz"):
            return gzip.open(f, mode=mode)
        else:
            return f
    else:
        return _fs_open(filepath, mode=mode, make_parents=True)


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
    input_filename: str, output_directory: str, disassemble=True, jsonify_content=True
) -> list:
    """
    Parses input file, writes outputs to output directory.

    Returns the dict of types to their output files.
    """
    open_output_files = {}
    with _open(input_filename) as f_in:
        releaseinfo = get_clinvar_xml_releaseinfo(f_in)
        release_date = releaseinfo["release_date"]
        _logger.debug(f"Parsing release date: {release_date}")

    # Release directory is within the output directory
    output_release_directory = f"{output_directory}/{release_date}"

    try:
        with _open(input_filename) as f_in:
            for obj in read_clinvar_xml(
                f_in, disassemble=disassemble, jsonify_content=jsonify_content
            ):
                entity_type = obj.entity_type
                f_out = get_open_file_for_writing(
                    open_output_files,
                    root_dir=output_release_directory,
                    label=entity_type,
                )
                obj_dict = dictify(obj)
                obj_dict["release_date"] = release_date
                f_out.write(json.dumps(obj_dict).encode("utf-8"))
                f_out.write("\n".encode("utf-8"))
    except Exception as e:
        _logger.critical("Exception caught in parse_and_write_files")
        raise e
    finally:
        _logger.debug("Closing output files")
        for f in open_output_files.values():
            f.close()

    return {k: v._name for k, v in open_output_files.items()}
