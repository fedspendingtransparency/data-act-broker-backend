import json
import flask
from flask import Flask, request, make_response, session, g, redirect, url_for, \
     abort, render_template, flash

class JsonResponse :
    """
    Constants for the response code
    """
    OK = 200
    ERROR  = 400
    LOGIN_REQUIRED = 401
    INTERNAL_ERROR = 500

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
