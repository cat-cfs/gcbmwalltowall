def require_not_null(string):
    if not string or not isinstance(string, str) or string.isspace():
        raise ValueError()

    return string
