from flask import request, session
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactbroker.handlers.accountHandler import AccountHandler
from dataactbroker.handlers.aws.session import LoginSession
from dataactcore.interfaces.interfaceHolder import InterfaceHolder

def add_login_routes(app,bcrypt):
    """ Create routes related to login """
    @app.route("/v1/login/", methods = ["POST"])
    def login():
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        accountManager.addInterfaces(InterfaceHolder())    # soon to be removed
        return accountManager.login(session)

    @app.route("/v1/max_login/", methods = ["POST"])
    def max_login():
        accountManager = AccountHandler(request)
        accountManager.addInterfaces(InterfaceHolder())    # soon to be removed
        return accountManager.max_login(session)

    @app.route("/v1/logout/", methods = ["POST"])
    def logout():
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        accountManager.addInterfaces(InterfaceHolder())    # soon to be removed
        return accountManager.logout(session)

    @app.route("/v1/session/", methods = ["GET"])
    def sessionCheck():
        session["session_check"] = True
        return JsonResponse.create(StatusCode.OK,{"status":str(LoginSession.isLogin(session))})
