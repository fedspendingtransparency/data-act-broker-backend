from flask import request, session

from dataactbroker.handlers.accountHandler import AccountHandler
from dataactbroker.handlers.aws.session import LoginSession
from dataactbroker.permissions import requires_admin, requires_login
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode


def add_user_routes(app,system_email,bcrypt):
    """ Create routes related to user management

        Args:
            app - Flask app to add routes to
            system_email - Sender address to use for emails
            bcrypt - Password hashing Bcrypt associated with app
    """

    @app.route("/v1/register/", methods = ["POST"])
    def register_user():
        """ Expects request to have keys 'email', 'name', 'cgac_code', and 'title' """
        # check the session to make sure register is set to prevent any one
        # from using route
        if LoginSession.isRegistering(session):
            accountManager = AccountHandler(request,bcrypt = bcrypt)
            return accountManager.register(system_email, session)
        else:
            return JsonResponse.create(StatusCode.LOGIN_REQUIRED,
                                       {'message': 'unauthorized'})

    @app.route("/v1/confirm_email/", methods = ["POST"])
    def confirm():
        """ Expects request to have email  """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return accountManager.create_email_confirmation(system_email)

    @app.route("/v1/confirm_email_token/", methods = ["POST"])
    def checkEmailToken():
        """ Expects request to have token  """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return accountManager.check_email_confirmation_token(session)

    @app.route("/v1/confirm_password_token/", methods = ["POST"])
    def checkPasswordToken():
        """ Expects request to have email  """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return accountManager.checkPasswordToken(session)

    @app.route("/v1/list_user_emails/", methods=["GET"])
    @requires_login
    def list_user_emails():
        """ list all users """
        accountManager = AccountHandler(request, bcrypt=bcrypt)
        return accountManager.list_user_emails()

    @app.route("/v1/list_users_with_status/", methods = ["POST"])
    @requires_admin
    def list_users_with_status():
        """ Expects request to have key 'status', will list all users with that status """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return accountManager.list_users_with_status()

    @app.route("/v1/set_password/", methods=["POST"])
    def set_password():
        """ Set a new password for specified user """
        if LoginSession.isResetingPassword(session):
            accountManager = AccountHandler(request,bcrypt = bcrypt)
            return accountManager.set_new_password(session)
        else:
            return JsonResponse.create(StatusCode.LOGIN_REQUIRED,
                                       {'message': 'unauthorized'})

    @app.route("/v1/reset_password/", methods=["POST"])
    def reset_password():
        """ Removes current password from DB and sends email with token for user to reset their password.  Expects 'email' key in request body. """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return accountManager.reset_password(system_email, session)

    @app.route("/v1/current_user/", methods=["GET"])
    @requires_login
    def current_user():
        """ gets the current user information """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return accountManager.get_current_user(session)

    @app.route("/v1/set_skip_guide/", methods=["POST"])
    @requires_login
    def set_skip_guide():
        """ Sets skip_guide param for current user """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return accountManager.set_skip_guide(session)

    @app.route("/v1/email_users/", methods=["POST"])
    @requires_login
    def email_users():
        """
        Sends email notifications to users that their submission is ready for review & publish viewing
        """
        accountManager = AccountHandler(request, bcrypt=bcrypt)
        return accountManager.email_users(system_email, session)
