from typing import Any, List


def extract_oneof(d: dict, *keys: List[Any]) -> Any:
    """
    For each key in `keys`, if key exists, remove it and return a
    tuple of the key and the value. If not, try the next key.

    If no item in keys exists in d, returns None
    """
    for k in keys:
        if k in d:
            return (k, d.pop(k))
    return None


def extract(d: dict, key: Any) -> Any:
    """
    If `key` is in `d`, remove it and return the value.
    """
    return d.pop(key, None)


def ensure_list(obj: Any):
    if not isinstance(obj, list):
        return [obj]
    return obj
