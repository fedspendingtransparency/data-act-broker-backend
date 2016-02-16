from flask import request, session
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactbroker.handlers.loginHandler import LoginHandler
from dataactbroker.handlers.aws.session import LoginSession

def add_login_routes(app,interfaces):
    @app.route("/v1/login/", methods = ["POST"])
    def login():
        loginManager = LoginHandler(request,interfaces)
        response = loginManager.login(session)
        return response

    @app.route("/v1/logout/", methods = ["POST"])
    def logout():
        loginManager = LoginHandler(request,interfaces)
        return loginManager.logout(session)

    @app.route("/v1/session/", methods = ["GET"])
    def sessionCheck():
        return JsonResponse.create(StatusCode.OK,{"status":str(LoginSession.isLogin(session))})
