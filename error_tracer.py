
import os
import inspect
import sys
from importlib import import_module

class ErrorTracer:

    def __init__(file_path: str):

        if not isinstance(file_path, str):
            raise TypeError()
        elif not os.path.isfile(file_path):
            raise ValueError()
        else:
            self.file_path = file_path

            import_module(
                name=self.file_path,
                package=None
            )

    def get_source_code(self):
        code = f"""
        try:
            {inspect.getsource(self.file_path)}
        except:
            traceback.print_exc()
         """

        return code



    def error_trace(exe_date : str) -> str:
        exec(
            source=self.get_source_code(), globals={ sys.args[0] : exe_date })



