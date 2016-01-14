import json
import flask
import sys
import traceback
from flask import Flask, request, make_response, session, g, redirect, url_for, \
     abort, render_template, flash
from dataactcore.utils.responseException import ResponseException

class JsonResponse :

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
            if(JsonResponse.logDebug):
                open("responseErrorLog","a").write(str(type(exception)))
                open("responseErrorLog","a").write(exception.message)
                open("responseErrorLog","a").write(trace)
            del exc_tb
            return JsonResponse.create(errorCode, responseDict)
        else:
            responseDict["message"] = "An error has occurred"
            return JsonResponse.create(errorCode, responseDict)
