
import flask
import json
from flask import Flask, request, make_response, session, g, redirect, url_for, \
     abort, render_template, flash
from handlers.loginHandler import LoginHandler
from handlers.aws.session import LoginSession
from handlers.utils.jsonResponse import JsonResponse
from handlers.utils.statusCode import StatusCode
def add_login_routes(app, loginManager):
    @app.route("/v1/login/", methods = ["POST"])
    def login():
        loginManager.setRequest(request)
        return loginManager.login(session)

    @app.route("/v1/logout/", methods = ["POST"])
    def logout():
        loginManager.setRequest(request)
        return loginManager.logout(session)

    @app.route("/v1/session/", methods = ["GET"])
    def sessionCheck():
        return JsonResponse.create(StatusCode.OK,{"status":str(LoginSession.isLogin(session))})
