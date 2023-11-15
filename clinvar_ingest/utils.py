def extract(d: dict, key, *else_keys):
    """
    Try to get `key` from `d`. If it exists, remove `key` from `d` and return the value.

    If it doesn't exist, repeat the process for each key in `else_keys`.
    """
    if key in d:
        val = d.pop(key)
        return val
    else:
        for ek in else_keys:
            ev = extract(d, ek)
            if ev:
                return ev
    return None
