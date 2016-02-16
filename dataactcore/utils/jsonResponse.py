import json
import flask
import sys
import traceback
from flask import Flask, request, make_response, session, g, redirect, url_for, \
     abort, render_template, flash
from dataactcore.utils.responseException import ResponseException

class JsonResponse :
    """ Used to create an http response object containing JSON """
    debugMode = True
    printDebug = False # Can cause errors when printing trace on ec2 if set to True
    logDebug = False

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
    def error(exception, errorCode, extraDict = {}):
        """ Create an http response object for specified error

        Args:
            exception: Exception to be represented by response object
            errorCode: Status code to be used in response
            extraDict: Extra fields and values to be included in response

        Returns:
            Http response object containing specified error
        """
        responseDict = {}
        for key in extraDict:
            responseDict[key] = extraDict[key]
        if(JsonResponse.debugMode):
            responseDict["message"] = str(exception)
            responseDict["errorType"] = str(type(exception))
            if(type(exception)==type(ResponseException("")) and exception.wrappedException != None):
                responseDict["wrappedType"] = str(type(exception.wrappedException))
                responseDict["wrappedMessage"] = str(exception.wrappedException)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            trace = traceback.extract_tb(exc_tb, 10)
            responseDict["trace"] = trace
            if(JsonResponse.printDebug):
                print(str(type(exception)))
                print(str(exception))
                print(str(trace))
            if(JsonResponse.logDebug):
                open("responseErrorLog","a").write(str(type(exception)))
                open("responseErrorLog","a").write(str(exception))
                open("responseErrorLog","a").write(str(trace))
            del exc_tb
            return JsonResponse.create(errorCode, responseDict)
        else:
            responseDict["message"] = "An error has occurred"
            return JsonResponse.create(errorCode, responseDict)
