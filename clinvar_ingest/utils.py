import time
from enum import StrEnum
from typing import Any


def extract_oneof(d: dict, *keys: Any) -> Any:
    """
    For each key in `keys`, if key exists, remove it and return a
    tuple of the key and the value. If not, try the next key.

    If no item in keys exists in d, returns None
    """
    for k in keys:
        if k in d:
            return (k, d.pop(k))
    return None


def extract(d: dict, *keys: Any) -> Any:
    """
    For the path of `keys`, get each value in succession. If any key does not exist,
    return None. If all keys exist, return the value of the last key.
    If the last key exists, remove it from its containing object.
    """
    for i, k in enumerate(keys):
        if d and k in d:
            if i == len(keys) - 1:
                return d.pop(k)
            d = d[k]
        else:
            return None
    return None


def get(d: dict, *keys: Any) -> Any:
    """
    Traverses the path of `keys` in `d` and returns the value.
    If any key does not exist, returns None.
    """
    for i, k in enumerate(keys):
        if d and k in d:
            if i == len(keys) - 1:
                return d[k]
            d = d[k]
        else:
            return None
    return None


def ensure_list(obj: Any) -> list:
    """
    Ensure that the given object is a list. If it is not a list, it will be
    wrapped in a list and returned. If it is already a list, it will be returned
    as is.

    Args:
        obj (Any): The object to ensure is a list.

    Returns:
        List: The object as a list.

    Example:
        >>> ensure_list('foo')
        ['foo']
        >>> ensure_list(['foo'])
        ['foo']
        >>> ensure_list(None)
        [None]

    This docstring above was added by copilot based on the code in the function.
    """
    if not isinstance(obj, list):
        return [obj]
    return obj


def flatten1(things: list[list[Any]]) -> list[Any]:
    """
    Takes a list of things. If any of the things are lists, they are flattened
    to the top level. The result is a list of things that is nested one fewer levels.

    Example:
        >>> flatten1([['foo'], ['bar']])
        ['foo', 'bar']
        >>> flatten1([['foo', 'bar'], ['baz']])
        ['foo', 'bar', 'baz']
        >>> flatten1(['foo', 'bar'])
        ['foo', 'bar']
        >>> flatten1([['foo'], 'bar'])
        ['foo', 'bar']
    """
    return [
        item
        for thing in things
        for item in (thing if isinstance(thing, list) else [thing])
    ]


def make_progress_logger(logger, fmt: str, max_value: int = 0, interval: int = 10):
    def log_progress(current_value, force=False):
        if getattr(log_progress, "prev_log_time", None) is None:
            log_progress.prev_log_time = time.time()
            log_progress.prev_value = 0
            log_progress.max_value = max_value
            return
        now = time.time()
        if force or now - log_progress.prev_log_time > interval:
            elapsed = now - log_progress.prev_log_time
            elapsed_value = current_value - log_progress.prev_value
            logger.info(
                fmt.format(
                    current_value=current_value,
                    elapsed=elapsed,
                    elapsed_value=elapsed_value,
                    max_value=log_progress.max_value,
                )
            )

            log_progress.prev_log_time = now
            log_progress.prev_value = current_value

    return log_progress


def make_counter():
    """
    Create a counter that starts at 0 and increments by 1 each time it is iterated on.

    Example:
        >>> counter = make_counter()
        >>> next(counter)
        0
        >>> next(counter)
        1
        >>> next(counter)
        2
    """
    i = 0
    while True:
        yield i
        i += 1


class ClinVarIngestFileFormat(StrEnum):
    VCV = "vcv"
    RCV = "rcv"
    BQ = "bq"
    SP = "sp"
