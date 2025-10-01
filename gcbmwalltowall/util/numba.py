import numpy as np
from numba import njit, types
from numba.typed import Dict


def numba_map(a: np.ndarray, m: dict) -> np.ndarray:
    """Return the mapped value of a according to the dictionary m.
    Any values in a not present as a key in m will be unchanged.
    The key and values of the dictionary m must be both of the same
    type as the array dtype, and the returned array will be the same
    type as the input array.
    Args:
        a (numpy.ndarray): a numpy array
        m (dict): a dictionary to map values in the resulting array
    Returns:
        numpy.ndarray: the numpy array with replaced mapped values
    """
    dict_type = types.__dict__[str(a.dtype)]
    d = Dict.empty(key_type=dict_type, value_type=dict_type)
    for k, v in m.items():
        d[k] = v
    return _numba_map(a, d)


@njit
def _numba_map(a, m):
    out = a.copy()
    for index, value in np.ndenumerate(a):
        out[index] = m[value]
    return out
