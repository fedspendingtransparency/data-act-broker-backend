import json
import flask
import sys
import traceback
from flask import Flask, request, make_response, session, g, redirect, url_for, \
     abort, render_template, flash
from dataactcore.utils.responseException import ResponseException

class JsonResponse :

    debugMode = True
    printDebug = False

    @staticmethod
    def create(code,dictionaryData):
        """
        Creates a JSON response object
        if debugMode is enabled errors are added
        """
        open("errorLog","a").write("Called create response\n")
        jsondata  =  flask.Response()
        jsondata.headers["Content-Type"] = "application/json"
        jsondata.status_code = code
        open("errorLog","a").write("Setting data\n")
        jsondata.set_data(json.dumps(dictionaryData))
        open("errorLog","a").write("Finished creating response\n")
        return jsondata

    @staticmethod
    def error(exception, errorCode, extraDict = {}):
        responseDict = {}
        for key in extraDict.iterkeys():
            responseDict[key] = extraDict[key]
        if(JsonResponse.debugMode):
            responseDict["message"] = exception.message
            responseDict["errorType"] = str(type(exception))
            if(type(exception)==type(ResponseException("")) and exception.wrappedException != None):
                responseDict["wrappedType"] = str(type(exception.wrappedException))
                responseDict["wrappedMessage"] = exception.wrappedException.message
            exc_type, exc_obj, exc_tb = sys.exc_info()
            trace = traceback.extract_tb(exc_tb, 10)
            responseDict["trace"] = trace
            if(JsonResponse.printDebug):
                print(str(type(exception)))
                print(exception.message)
                print(trace)
            del exc_tb
            return JsonResponse.create(errorCode, responseDict)
        else:
            responseDict["message"] = "An error has occurred"
            return JsonResponse.create(errorCode, responseDict)
