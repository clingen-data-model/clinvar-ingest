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


def extract_in(d: dict, *keys: List[Any]) -> Any:
    """
    For the path of `keys`, get each value in succession. If any key does not exist,
    return None. If all keys exist, return the value of the last key.
    If the last key exists, remove it from its containing object.
    """
    for i, k in enumerate(keys):
        if d and k in d:
            if i == len(keys) - 1:
                return d.pop(k)
            else:
                d = d[k]
        else:
            return None


def extract(d: dict, key: Any) -> Any:
    """
    If `key` is in `d`, remove it and return the value.
    """
    if d is not None:
        return d.pop(key, None)


def get(d: dict, *keys: List[Any]) -> Any:
    """
    Traverses the path of `keys` in `d` and returns the value.
    If any key does not exist, returns None.
    """
    for i, k in enumerate(keys):
        if d and k in d:
            if i == len(keys) - 1:
                return d[k]
            else:
                d = d[k]
        else:
            return None


def ensure_list(obj: Any) -> List:
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


def flatten1(things: List[List[Any]]) -> List[Any]:
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
    outputs = []
    for thing in things:
        if isinstance(thing, list):
            for item in thing:
                outputs.append(item)
        else:
            outputs.append(thing)
    return outputs
