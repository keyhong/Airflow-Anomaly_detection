
__all__ = ['SqoopError', 'HiveError', 'PythonError']

from typing import Dict

class SqoopError:

    __type_1 = 'N killed'
    __type_2 = 'Read-only file system'


class HiveError:

    __SemanticException = 'SemanticException'
    __OutOfMemoryError = 'java.lang.OutOfMemoryError'
    __FileNotFoundException = 'java.io.FileNotFoundException'
    __ExecutionError = 'Execution Error'

    @property
    def SemanticException(self):
        # return code   
        return self.__SemanticException

    @property
    def FileNotFoundException(self):
        # return code   
        return self.__FileNotFoundException

    @property
    def OutOfMemoryError(self):
        # return code
        return self.__OutOfMemoryError

    @property
    def ExecutionError(self):
        # return code
        return self.__ExecutionError

    @property
    def exceptions(self) -> Dict[str, str]:
        return {
            'SemanticException' : self.SemanticException,
            'FileNotFoundException' : self.FileNotFoundException,
            'OutOfMemoryError' : self.OutOfMemoryError,
            'ExecutionError'     : self.ExecutionError
        }
    

class PythonError:

    __type_1 = 'TypeError'
    __type_2 = 'AttributeError'
    __type_1 = 'java.lang.OutOfMemoryError'


