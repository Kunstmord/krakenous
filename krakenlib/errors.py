__author__ = 'George Oblapenko, Viktor Evstratov'
__license__ = "GPLv3"
# Custom erros (no backend found, no such id, cannot add to db, field with this name already exists, etc) go here


class UnsupportedOperation(Exception):
    def __init__(self, operation: str, backend: str):
        Exception.__init__(self)
        self.operation = operation
        self.backend = backend
        
    def __str__(self):
        return 'Operation ' + self.operation + ' not supported for ' + self.backend + ' backend'


class OverwriteError(Exception):
    def __init__(self, to_be_overwritten_name: str):
        Exception.__init__(self)
        self.to_be_overwritten_name = to_be_overwritten_name

    def __str__(self):
        return 'Could not overwrite "' + self.to_be_overwritten_name + '"'


class UnknownBackend(Exception):
    def __init__(self, backend_name: str):
        Exception.__init__(self)
        self.backend_name = backend_name

    def __str__(self):
        return 'Unkown backend "' + self.backend_name + '"'