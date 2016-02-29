import logging
import logstash
import os
import json
import inspect
from dataactcore.utils.responseException import ResponseException

class CloudLogger(object):
    """ Singleton Logging object """
    LOGGER = None

    @staticmethod
    def getValueFromConfig(value):
        """ Retrieve specified value from config file """
        path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        loggingConfig = open(path+"/logging.json","r").read()
        bucketDict = json.loads(loggingConfig)
        return bucketDict[value]

    @staticmethod
    def getLogger():
        """gets current logger"""
        if(CloudLogger.LOGGER == None):
            CloudLogger.LOGGER = logging.getLogger('python-logstash-logger')
            CloudLogger.LOGGER.setLevel(logging.INFO)
            CloudLogger.LOGGER.addHandler(logstash.LogstashHandler(CloudLogger.getValueFromConfig("host"), CloudLogger.getValueFromConfig("port"), version=1))
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
            'error_log_wrapped_message': wrappedMessage,
            'error_log_wrapped_type': wrappedType,
            'error_log_trace': traceback
        }
        CloudLogger.getLogger().error("".join([message,str(exception)]),extra=logging_helpers)
