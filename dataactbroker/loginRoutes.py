from flask import request, session
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactbroker.handlers.accountHandler import AccountHandler
from dataactbroker.handlers.aws.session import LoginSession
from dataactbroker.routeUtils import RouteUtils

def add_login_routes(app,bcrypt):
    """ Create routes related to login """
    @app.route("/v1/login/", methods = ["POST"])
    def login():
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return RouteUtils.run_instance_function(accountManager, accountManager.login, session)

    @app.route("/v1/max_login/", methods = ["POST"])
    def max_login():
        accountManager = AccountHandler(request)
        return RouteUtils.run_instance_function(accountManager, accountManager.max_login, session)

    @app.route("/v1/logout/", methods = ["POST"])
    def logout():
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return RouteUtils.run_instance_function(accountManager, accountManager.logout, session)

    @app.route("/v1/session/", methods = ["GET"])
    def sessionCheck():
        session["session_check"] = True
        return JsonResponse.create(StatusCode.OK,{"status":str(LoginSession.isLogin(session))})
