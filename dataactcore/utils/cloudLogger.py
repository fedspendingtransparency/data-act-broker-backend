import logging
import logstash
import os
import json
from dataactcore.utils.responseException import ResponseException
from dataactcore.config import CONFIG_LOGGING

class CloudLogger(object):
    """Singleton Logging object."""
    LOGGER = None

    @staticmethod
    def getLogger():
        """Get current logger."""
        if not CloudLogger.LOGGER:
            CloudLogger.LOGGER = logging.getLogger('python-logstash-logger')
            CloudLogger.LOGGER.setLevel(logging.INFO)
            CloudLogger.LOGGER.addHandler(logstash.LogstashHandler(CONFIG_LOGGING["logstash_host"], CONFIG_LOGGING["logstash_port"], version=1))
        return CloudLogger.LOGGER

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
            'error_log_trace': str(traceback)
        }
        if CONFIG_LOGGING["use_logstash"]:
        #if( not CloudLogger.getValueFromConfig("local")):
            CloudLogger.getLogger().error(
                "".join([message, str(exception)]), extra=logging_helpers)
        else:
            localPath = os.path.join(CONFIG_LOGGING["log_files"], "error.log")
            with open(localPath, "a") as file:
                file.write("\n\n".join(["\n\n", message,
                    str(exception), json.dumps(logging_helpers)]))
            #open (localPath,"a").write("\n\n".join(["\n\n",message,str(exception),json.dumps(logging_helpers)]))
