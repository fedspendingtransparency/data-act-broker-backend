
import flask
import json
from flask import Flask, request, make_response, session, g, redirect, url_for, \
     abort, render_template, flash
from handlers.loginHandler import LoginHandler
from handlers.aws.session import LoginSession
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
def add_login_routes(app):
    @app.route("/v1/login/", methods = ["POST"])
    def login():
        open("errorLog","a").write("Called login route\n")
        loginManager = LoginHandler(request)
        return loginManager.login(session)

    @app.route("/v1/logout/", methods = ["POST"])
    def logout():
        open("errorLog","a").write("Called logout route\n")
        loginManager = LoginHandler(request)
        return loginManager.logout(session)

    @app.route("/v1/session/", methods = ["GET"])
    def sessionCheck():
        open("errorLog","a").write("Called session check route\n")
        return JsonResponse.create(StatusCode.OK,{"status":str(LoginSession.isLogin(session))})
