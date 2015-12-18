import sys
import flask
from threading import Thread
from flask import Flask, request, make_response, session, g, redirect, url_for, \
     abort, render_template, flash ,session, Response, copy_current_request_context
import json
print(sys.path)
open("pathLog","w").write(str(sys.path))
from handlers.validationManager import ValidationManager
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode

debugFlag = True

# Create application
app = Flask(__name__)
app.config.from_object(__name__)

validationManager = ValidationManager()



@app.route("/validate_threaded/",methods=["POST"])
def validate_threaded():

    @copy_current_request_context
    def ThreadedFunction ()  :
        threadedManager = ValidationManager()
        threadedManager.validateJob(flask.request)

    thread = Thread(target=ThreadedFunction)
    thread.start()
    return JsonResponse.create(StatusCode.OK,{"table":"TESTING"})

@app.route("/validate/",methods=["POST"])
def validate():
    return validationManager.validateJob(request)

if __name__ == '__main__':
    app.run(debug=debugFlag)
