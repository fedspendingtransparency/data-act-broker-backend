import sys
#print(sys.version)
from flask import Flask, request, make_response, session, g, redirect, url_for, \
     abort, render_template, flash ,session
import json
import flask
from loginHandler import LoginHandler
from aws.session import DynamoInterface, SessionTable, LoginSession

# Set parameters
debugFlag = True # Should be false for prod

# Create application
app = Flask(__name__)
app.config.from_object(__name__)
#Enable AWS Sessions
app.session_interface = DynamoInterface()
#login route, will take either application/json or application/x-www-form-urlencoded
@app.route("/v1/login/", methods = ["POST"])
def login():
    response = flask.Response()
    loginManager = LoginHandler(request, response)
    return loginManager.login(session)

@app.route("/v1/logout/", methods = ["GET"])
def logout():
    response = flask.Response()
    loginManager = LoginHandler(request, response)
    return loginManager.logout(session)

@app.route("/v1/session/", methods = ["GET"])
def sessionCheck():
    response = flask.Response()
    response.headers["Content-Type"] = "application/json"
    response.status_code = 200
    response.set_data(json.dumps({"status":str(LoginSession.isLogin(session))}))
    return response

if __name__ == '__main__':
    SessionTable.setup(app,True,False)
    app.run(debug=debugFlag)
