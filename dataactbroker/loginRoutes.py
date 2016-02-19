from flask import request, session
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactbroker.handlers.interfaceHolder import InterfaceHolder
from dataactbroker.handlers.accountHandler import AccountHandler
from dataactbroker.handlers.aws.session import LoginSession

def add_login_routes(app):
    @app.route("/v1/login/", methods = ["POST"])
    def login():
        interfaces = InterfaceHolder()
        loginManager = AccountHandler(request, interfaces)
        response = loginManager.login(session)
        interfaces.close()
        return response

    @app.route("/v1/logout/", methods = ["POST"])
    def logout():
        interfaces = InterfaceHolder()
        loginManager = AccountHandler(request, interfaces)
        interfaces.close()
        return loginManager.logout(session)

    @app.route("/v1/session/", methods = ["GET"])
    def sessionCheck():
        return JsonResponse.create(StatusCode.OK,{"status":str(LoginSession.isLogin(session))})
