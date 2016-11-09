import json
import logging
import traceback

import flask

from dataactcore.utils.responseException import ResponseException


_exception_logger = logging.getLogger('deprecated.exception')


class JsonResponse :
    """ Used to create an http response object containing JSON """
    debugMode = True

    @staticmethod
    def create(code,dictionaryData):
        """
        Creates a JSON response object
        if debugMode is enabled errors are added
        """
        jsondata  =  flask.Response()
        jsondata.headers["Content-Type"] = "application/json"
        jsondata.status_code = code
        jsondata.set_data(json.dumps(dictionaryData))
        return jsondata

    @staticmethod
    def error(exception, errorCode, **kwargs):
        """ Create an http response object for specified error. We assume
        we're in an exception context

        Args:
            exception: Exception to be represented by response object
            errorCode: Status code to be used in response
            kwargs: Extra fields and values to be included in response

        Returns:
            Http response object containing specified error
        """
        responseDict = {}
        for key in kwargs:
            responseDict[key] = kwargs[key]


        trace = traceback.extract_tb(exception.__traceback__, 10)
        _exception_logger.exception('Route Error')
        # TODO: that this is eerily similar to CloudLogger / 
        # DeprecatedJSONFormatter. We may want to remove this method
        if JsonResponse.debugMode:
            responseDict["message"] = str(exception)
            responseDict["errorType"] = str(type(exception))
            if (isinstance(exception, ResponseException) and
                    exception.wrappedException):
                responseDict["wrappedType"] = str(type(exception.wrappedException))
                responseDict["wrappedMessage"] = str(exception.wrappedException)
            responseDict["trace"] = [str(entry) for entry in trace]
            return JsonResponse.create(errorCode, responseDict)
        else:
            responseDict["message"] = "An error has occurred"
            return JsonResponse.create(errorCode, responseDict)
