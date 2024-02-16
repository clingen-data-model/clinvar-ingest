import gzip
import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import PurePath
from typing import List


@dataclass
class FileListing:
    proto = "file://"
    files = []


class BinaryOpenMode(StrEnum):
    READ = "rb"
    WRITE = "wb"


def assert_mkdir(db_directory: str):
    if not os.path.exists(db_directory):
        os.mkdir(db_directory)
    elif not os.path.isdir(db_directory):
        raise OSError(f"Path exists but is not a directory!: {db_directory}")


def find_files(root_directory: str) -> List[str]:
    """
    Find all files (not directories) in `root_directory` and return their paths.

    Returned paths will all be relative to `root_directory`, not including `root_directory`.

    e.g.

    mkdir -p A/B/C && touch A/B/C/D

    find_files("A") -> ["B/C/D"]
    """
    outputs = []
    for dirpath, dirnames, filenames in os.walk(root_directory):
        # Directory prefix relative to root_directory (not including it)
        relativized_dir_path = dirpath[len(root_directory) :]
        if relativized_dir_path.startswith("/"):
            relativized_dir_path = relativized_dir_path[1:]
        if len(filenames):
            for filename in filenames:
                filepath = f"{relativized_dir_path}/{filename}"
                outputs.append(filepath)
    return outputs


class ReadCounter:
    def __init__(self, f):
        self.f = f
        self.bytes_read = 0

    def __getattr__(self, name):
        return getattr(self.f, name)

    def read(self, size=-1):
        result = self.f.read(size)
        assert isinstance(result, bytes), "ReadCounter only works with binary files."
        self.bytes_read += len(result)
        return result

    def tell(self):
        return self.bytes_read


def fs_open(
    filename: str, make_parents=True, mode: BinaryOpenMode = BinaryOpenMode.READ
):
    """
    Opens a file with path `filename`. If `filename` ends in .gz, opens as gzip.

    If `make_parents` is True, creates parent directories if they do not exist.
    """
    if make_parents:
        for parent in reversed(PurePath(filename).parents):
            assert_mkdir(parent)
    if filename.endswith(".gz"):
        return gzip.open(filename, mode)
    return open(filename, mode=mode)  # pylint: disable=W1514
