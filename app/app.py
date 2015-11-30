import sys
#print(sys.version)
from flask import Flask, request, make_response, session, g, redirect, url_for, \
     abort, render_template, flash
import json
import flask
from loginHandler import LoginHandler

# Set parameters
debugFlag = True # Should be false for prod

# Create application
app = Flask(__name__)
app.config.from_object(__name__)

#login route, will take either application/json or application/x-www-form-urlencoded
@app.route("/v1/login/", methods = ['POST'])
def login():
    response = flask.Response()
    loginManager = LoginHandler(request, response)
    return loginManager.login()

@app.route("/test/")
def test():
    response = flask.Response()
    response.status_code = 200
    response.headers.add("Content-Type","application/json")
    respJson = json.dumps({"message":"Test successful"})
    response.set_data(respJson)
    return response

if __name__ == '__main__':
    app.run(debug=debugFlag)