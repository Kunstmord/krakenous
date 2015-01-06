__author__ = 'George Oblapenko'
"""
Misc. helper functions
"""

import numpy as np
from krakenous.errors import KrakenousException


def is_a_number(x):
    if isinstance(x, int) or isinstance(x, float) or isinstance(x, complex) or isinstance(x, np.number):
        return True
    else:
        return False


def element_length(x):
    result_length = -1
    if isinstance(x, np.ndarray):
        np_size = 1
        for i in x.shape:
            np_size *= i
        result_length = (np_size, 'ndarray')
    elif isinstance(x, list):
        result_length = (len(x), 'list')
    elif isinstance(x, tuple):
        result_length = (len(x), 'tuple')
    elif is_a_number(x):
        result_length = (1, 'number')
    elif isinstance(x, str):
        result_length = ('string', 'string')
    else:
        raise KrakenousException('Unknown type')
    return result_length