from flask import g, request

from dataactbroker.handlers.account_handler import AccountHandler, json_for_user, list_user_emails
from dataactbroker.permissions import requires_login
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode


def add_user_routes(app, system_email, bcrypt):
    """ Create routes related to user management

        Args:
            app - Flask app to add routes to
            system_email - Sender address to use for emails
            bcrypt - Password hashing Bcrypt associated with app
    """

    @app.route("/v1/list_user_emails/", methods=["GET"])
    @requires_login
    def list_user_emails_route():
        """ list all users """
        return list_user_emails()

    @app.route("/v1/current_user/", methods=["GET"])
    @requires_login
    def current_user():
        """ gets the current user information """
        return JsonResponse.create(StatusCode.OK, json_for_user(g.user))

    @app.route("/v1/set_skip_guide/", methods=["POST"])
    @requires_login
    def set_skip_guide():
        """ Sets skip_guide param for current user """
        account_manager = AccountHandler(request, bcrypt=bcrypt)
        return account_manager.set_skip_guide()

    @app.route("/v1/email_users/", methods=["POST"])
    @requires_login
    def email_users():
        """
        Sends email notifications to users that their submission is ready for review & publish viewing
        """
        account_manager = AccountHandler(request, bcrypt=bcrypt)
        return account_manager.email_users(system_email)
