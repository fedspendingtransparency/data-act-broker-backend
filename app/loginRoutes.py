
import flask
import json
from flask import Flask, request, make_response, session, g, redirect, url_for, \
     abort, render_template, flash
from handlers.loginHandler import LoginHandler
from handlers.aws.session import LoginSession
from handlers.utils.jsonResponse import JsonResponse
def add_login_routes(app):
    @app.route("/v1/login/", methods = ["POST"])
    def login():
        loginManager = LoginHandler(request)
        return loginManager.login(session)

    @app.route("/v1/logout/", methods = ["POST"])
    def logout():
        loginManager = LoginHandler(request)
        return loginManager.logout(session)

    @app.route("/v1/session/", methods = ["GET"])
    def sessionCheck():
        return JsonResponse.create(JsonResponse.OK,{"status":str(LoginSession.isLogin(session))})
