from flask import request, session
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactbroker.handlers.interfaceHolder import InterfaceHolder
from dataactbroker.handlers.accountHandler import AccountHandler
from dataactbroker.permissions import permissions_check

# Add the file submission route
def add_user_routes(app,system_email,bcrypt):
    """ Create routes related to file submission for flask app

    """
    SYSTEM_EMAIL = system_email


    def run_account_function(accountManager, accountFunction, getSystemEmail = False, getSession = False):
        """ Standard error handling around each route """
        interfaces = InterfaceHolder()
        try:
            accountManager.addInterfaces(interfaces)
            if(getSystemEmail and getSession):
                return accountFunction(SYSTEM_EMAIL,session)
            if(getSystemEmail):
                return accountFunction(SYSTEM_EMAIL)
            elif(getSession):
                return accountFunction(session)
            else:
                return accountFunction()
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
            return JsonResponse.error(exc,exc.status)
        finally:
            interfaces.close()

    @app.route("/v1/register/", methods = ["POST"])
    #check the session to make sure register is set to prevent any one from using route
    #@permissions_check # TODO require token
    def register_user():
        """ Expects request to have keys 'email', 'name', 'agency', and 'title' """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return run_account_function(accountManager,accountManager.register, True,True)

    @app.route("/v1/change_status/", methods = ["POST"])
    @permissions_check # TODO require admin
    def change_status():
        """ Expects request to have keys 'user_email' and 'new_status' """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return run_account_function(accountManager,accountManager.changeStatus)

    @app.route("/v1/confirm_email/", methods = ["POST"])
    def confirm():
        """ Expects request to have email  """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return run_account_function(accountManager,accountManager.createEmailConfirmation,True)

    @app.route("/v1/confirm_email_token/", methods = ["POST"])
    @permissions_check #TODO require token
    def checkToken():
        """ Expects request to have email  """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return run_account_function(accountManager,accountManager.checkEmailConfirmation,getSession = True)

    @app.route("/v1/list_users_with_status/", methods = ["POST"])
    @permissions_check # TODO require admin
    def list_users_with_status():
        """ Expects request to have key 'status', will list all users with that status """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return run_account_function(accountManager,accountManager.listUsersWithStatus)

    @app.route("/v1/list_submissions/", methods = ["GET"])
    @permissions_check
    def list_submissions():
        """ List submission IDs associated with the current user """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return run_account_function(accountManager,accountManager.listSubmissionsByCurrentUser)

    @app.route("/v1/set_password/", methods=["POST"])
    @permissions_check #TODO require token
    def set_password():
        """ Set a new password for specified user """
        accountManager = AccountHandler(request,bcrypt = bcrypt)
        return run_account_function(accountManager,accountManager.setNewPassword)
