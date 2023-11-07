import os
from dataclasses import dataclass


@dataclass
class FileListing:
    proto = "file://"
    files = []


def assert_mkdir(db_directory: str):
    if not os.path.exists(db_directory):
        os.mkdir(db_directory)
    elif not os.path.isdir(db_directory):
        raise OSError(f"Path exists but is not a directory!: {db_directory}")
