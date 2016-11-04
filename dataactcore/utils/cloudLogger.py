import os
import json
from datetime import datetime
from dataactcore.utils.responseException import ResponseException
from dataactcore.read_config import CONFIG_LOGGING

class CloudLogger(object):
    """Singleton Logging object."""
    @staticmethod
    def logError(message,exception,traceback):
        """Logs errors"""
        wrappedType =""
        wrappedMessage =""
        if(type(exception)==type(ResponseException("")) and exception.wrappedException != None):
            wrappedType = str(type(exception.wrappedException))
            wrappedMessage= str(exception.wrappedException)
        logging_helpers = {
            'error_log_type': str(type(exception)),
            'error_log_message': str(exception),
            'error_log_wrapped_message': str(wrappedMessage),
            'error_log_wrapped_type': str(wrappedType),
            'error_log_trace': str(traceback),
            'error_timestamp': str(datetime.utcnow())
        }
        path = CONFIG_LOGGING["log_files"]
        if not os.path.exists(path):
            os.makedirs(path)
        localFile = os.path.join(path, "error.log")
        with open(localFile, "a") as file:
            file.write("{} {} {}\n".format(message,
                str(exception), json.dumps(logging_helpers)))
