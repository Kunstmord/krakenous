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