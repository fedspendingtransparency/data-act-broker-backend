from flask import g, request, session

from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactbroker.handlers.account_handler import AccountHandler, logout


def add_login_routes(app, is_local, bcrypt):
    """Create routes related to login"""

    @app.route("/v1/login/", methods=["POST"])
    def login():
        if not is_local:
            return JsonResponse.error("Login route can only be used locally.", StatusCode.CLIENT_ERROR)
        account_manager = AccountHandler(request, bcrypt=bcrypt)
        return account_manager.login(session)

    @app.route("/v1/proxy_login/", methods=["POST"])
    def proxy_login():
        account_manager = AccountHandler(request)
        return account_manager.proxy_login(session)

    @app.route("/v1/caia_login/", methods=["POST"])
    def caia_login():
        account_manager = AccountHandler(request)
        return account_manager.caia_login(session)

    @app.route("/v1/logout/", methods=["POST"])
    def logout_user():
        return logout(session)

    @app.route("/v1/session/", methods=["GET"])
    def session_check():
        session["session_check"] = True
        return JsonResponse.create(StatusCode.OK, {"status": str(g.user is not None)})
