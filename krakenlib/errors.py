__author__ = 'George Oblapenko, Viktor Evstratov'
__license__ = "GPLv3"
# Custom errors (no backend found, no such id, cannot add to db, field with this name already exists, etc) go here


class KrakenlibException(Exception):
    pass