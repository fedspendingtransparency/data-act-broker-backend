from flask import request
from dataactbroker.handlers.accountHandler import AccountHandler
from dataactbroker.permissions import permissions_check
from dataactbroker.routeUtils import RouteUtils


def add_user_routes(app,system_email,bcrypt):
    """ Create routes related to file submission for flask app

    """

    RouteUtils.SYSTEM_EMAIL = system_email # Set the system email to be used

    @app.route("/v1/register/", methods = ["POST"])
    #check the session to make sure register is set to prevent any one from using route
    @permissions_check(permissionList=["check_email_token"])
    def register_user():
        """ Expects request to have keys 'email', 'name', 'agency', and 'title' """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return RouteUtils.run_instance_function(accountManager,accountManager.register, getSystemEmail = True, getSession = True)


    @app.route("/v1/change_status/", methods = ["POST"])
    @permissions_check(permissionList=["website_admin"])
    def change_status():
        """ Expects request to have keys 'user_email' and 'new_status' """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return RouteUtils.run_instance_function(accountManager, accountManager.changeStatus,getSystemEmail = True)

    @app.route("/v1/confirm_email/", methods = ["POST"])
    def confirm():
        """ Expects request to have email  """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return RouteUtils.run_instance_function(accountManager, accountManager.createEmailConfirmation, True,True)

    @app.route("/v1/confirm_email_token/", methods = ["POST"])
    def checkEmailToken():
        """ Expects request to have token  """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return RouteUtils.run_instance_function(accountManager, accountManager.checkEmailConfirmationToken, getSession = True)


    @app.route("/v1/confirm_password_token/", methods = ["POST"])
    def checkPasswordToken():
        """ Expects request to have email  """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return RouteUtils.run_instance_function(accountManager, accountManager.checkPasswordToken, getSession = True)


    @app.route("/v1/list_users_with_status/", methods = ["POST"])
    @permissions_check(permissionList=["website_admin"])
    def list_users_with_status():
        """ Expects request to have key 'status', will list all users with that status """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return RouteUtils.run_instance_function(accountManager, accountManager.listUsersWithStatus)

    @app.route("/v1/set_password/", methods=["POST"])
    @permissions_check(permissionList=["check_password_token"])
    def set_password():
        """ Set a new password for specified user """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return RouteUtils.run_instance_function(accountManager, accountManager.setNewPassword, getSession = True)

    @app.route("/v1/reset_password/", methods=["POST"])
    def reset_password():
        """ Removes current password from DB and sends email with token for user to reset their password.  Expects 'email' key in request body. """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return RouteUtils.run_instance_function(accountManager, accountManager.resetPassword, True,True)

    @app.route("/v1/current_user/", methods=["GET"])
    @permissions_check
    def current_user():
        """ gets the current user information """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return RouteUtils.run_instance_function(accountManager, accountManager.getCurrentUser, getSession = True)
