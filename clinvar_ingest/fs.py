import os
from dataclasses import dataclass
from typing import List


@dataclass
class FileListing:
    proto = "file://"
    files = []


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
