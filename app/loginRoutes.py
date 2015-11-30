
import flask
import json
from flask import Flask, request, make_response, session, g, redirect, url_for, \
     abort, render_template, flash
from handlers.loginHandler import LoginHandler
from handlers.aws.session import LoginSession
def add_login_routes(app):
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
