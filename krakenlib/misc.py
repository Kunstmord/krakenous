__author__ = 'George Oblapenko'
"""
Misc. helper functions
"""

import numpy as np


def is_a_number(x):
    if isinstance(x, int) or isinstance(x, float) or isinstance(x, complex) or isinstance(x, np.number):
        return True
    else:
        return False