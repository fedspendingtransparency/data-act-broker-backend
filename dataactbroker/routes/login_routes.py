from flask import g, request, session

from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactbroker.handlers.account_handler import AccountHandler, logout


def add_login_routes(app, bcrypt):
    """ Create routes related to login """
    @app.route("/v1/login/", methods=["POST"])
    def login():
        account_manager = AccountHandler(request, bcrypt=bcrypt)
        return account_manager.login(session)

    @app.route("/v1/max_login/", methods=["POST"])
    def max_login():
        account_manager = AccountHandler(request)
        return account_manager.max_login(session)

    @app.route("/v1/logout/", methods=["POST"])
    def logout_user():
        return logout(session)

    @app.route("/v1/session/", methods=["GET"])
    def session_check():
        session["session_check"] = True
        return JsonResponse.create(StatusCode.OK, {"status": str(g.user is not None)})
