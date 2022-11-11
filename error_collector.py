#!/bin/usr/env python3

__all__ = ['SqoopError', 'HiveError', 'PythonError']

from typing import Dict

class SqoopError:

    __EOFException = 'java.io.EOFException'
    __FileAlreadyExistsException = 'org.apache.hadoop.mapred.FileAlreadyExistsException'
    __ParsingError = 'Error parsing'

    @property
    def EOFException(self):
        return self.__EOFException

    @property
    def FileAlreadyExistsException(self):
        return self.__FileAlreadyExistsException

    @property
    def ParsingError(self):
        return self.__ParsingError

    @property
    def exceptions(self) -> Dict[str, str]:
        return {
            'EOFException' : self.EOFException,
            'FileAlreadyExistsException' : self.FileAlreadyExistsException,
            'ParsingError' : self.ParsingError
        }
    



class HiveError:

    __SemanticException = 'SemanticException'
    __OutOfMemoryError = 'java.lang.OutOfMemoryError'
    __FileNotFoundException = 'java.io.FileNotFoundException'
    __ExecutionError = 'Execution Error'

    @property
    def SemanticException(self):
        return self.__SemanticException

    @property
    def FileNotFoundException(self):
        return self.__FileNotFoundException

    @property
    def OutOfMemoryError(self):
        return self.__OutOfMemoryError

    @property
    def ExecutionError(self):
        return self.__ExecutionError

    @property
    def exceptions(self) -> Dict[str, str]:
        return {
            'SemanticException' : self.SemanticException,
            'FileNotFoundException' : self.FileNotFoundException,
            'OutOfMemoryError' : self.OutOfMemoryError,
            'ExecutionError' : self.ExecutionError
        }
    

class PythonError:

    __type_1 = 'TypeError'
    __type_2 = 'AttributeError'
    __type_1 = 'java.lang.OutOfMemoryError'


