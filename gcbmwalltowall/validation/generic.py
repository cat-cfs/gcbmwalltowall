def require_instance_of(obj, type):
    if not isinstance(obj, type):
        raise TypeError(f"expected {type}, got {type(obj)}")

    return obj
