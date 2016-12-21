from flask import g, request, session

from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactbroker.handlers.accountHandler import AccountHandler, logout


def add_login_routes(app,bcrypt):
    """ Create routes related to login """
    @app.route("/v1/login/", methods = ["POST"])
    def login():
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return accountManager.login(session)

    @app.route("/v1/max_login/", methods = ["POST"])
    def max_login():
        accountManager = AccountHandler(request)
        return accountManager.max_login(session)

    @app.route("/v1/logout/", methods = ["POST"])
    def logout_user():
        return logout(session)

    @app.route("/v1/session/", methods = ["GET"])
    def sessionCheck():
        session["session_check"] = True
        return JsonResponse.create(StatusCode.OK,
                                   {"status": str(g.user is not None)})
