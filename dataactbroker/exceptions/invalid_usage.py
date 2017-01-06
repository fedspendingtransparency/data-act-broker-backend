import sys
import traceback
from dataactcore.utils.jsonResponse import JsonResponse


class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        if JsonResponse.debugMode:
            exception_type, _, trace = sys.exc_info()
            trace = traceback.extract_tb(trace, 10)
            rv['exception_type'], rv['trace'] = str(exception_type), str(trace)
        return rv
