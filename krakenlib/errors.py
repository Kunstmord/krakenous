__author__ = 'George Oblapenko, Viktor Evstratov'
__license__ = "GPLv3"
# Custom erros (no backend found, no such id, cannot add to db, field with this name already exists, etc) go here


class UnsupportedOperation(Exception):
    pass


class OverwriteError(Exception):
    pass


class UnknownBackend(Exception):
    pass


class DoesNotExist(Exception):
    pass


class DataPropertyNameCollision(Exception):
    pass


class ProtectedField(Exception):
    pass


class KrakenlibException(Exception):
    pass


class EmptyDataSet(Exception):
    pass


class InconsistentFeatureTypes(Exception):
    pass


class NonNumericFeatureType(Exception):
    pass


class IncorrectRange(Exception):
    pass