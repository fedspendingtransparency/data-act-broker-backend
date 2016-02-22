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

    @app.route("/v1/register/", methods = ["POST"])
    #check the session to make sure register is set to prevent any one from using route
    @permissions_check(permissionList=["check_email_token"])
    def register_user():
        """ Expects request to have keys 'email', 'name', 'agency', and 'title' """
        interfaces = InterfaceHolder()
        try:
            accountManager = AccountHandler(request,interfaces,bcrypt)
            return accountManager.register(SYSTEM_EMAIL,session)
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
            return JsonResponse.error(exc,exc.status)
        finally:
            interfaces.close()

    @app.route("/v1/change_status/", methods = ["POST"])
    @permissions_check # TODO require admin
    def change_status():
        """ Expects request to have keys 'user_email' and 'new_status' """
        interfaces = InterfaceHolder()
        try:
            accountManager = AccountHandler(request,interfaces,bcrypt)
            return accountManager.changeStatus()
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
            return JsonResponse.error(exc,exc.status)
        finally:
            interfaces.close()

    @app.route("/v1/confirm_email/", methods = ["POST"])
    def confirm():
        """ Expects request to have email  """
        interfaces = InterfaceHolder()
        try:
            accountManager = AccountHandler(request,interfaces,bcrypt)
            return accountManager.createEmailConfirmation(SYSTEM_EMAIL)
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
            return JsonResponse.error(exc,exc.status)
        finally:
            interfaces.close()

    @app.route("/v1/confirm_email_token/", methods = ["POST"])
    def checkToken():
        """ Expects request to have email  """
        interfaces = InterfaceHolder()
        try:
            accountManager = AccountHandler(request,interfaces,bcrypt)
            return accountManager.checkEmailConfirmation(session)
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
            return JsonResponse.error(exc,exc.status)
        finally:
            interfaces.close()


    @app.route("/v1/list_users_with_status/", methods = ["POST"])
    @permissions_check # TODO require admin
    def list_users_with_status():
        """ Expects request to have key 'status', will list all users with that status """
        interfaces = InterfaceHolder()
        try:
            accountManager = AccountHandler(request,interfaces,bcrypt)
            return accountManager.listUsersWithStatus()
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
            return JsonResponse.error(exc,exc.status)
        finally:
            interfaces.close()

    @app.route("/v1/list_submissions/", methods = ["GET"])
    @permissions_check
    def list_submissions():
        """ List submission IDs associated with the current user """
        interfaces = InterfaceHolder()
        try:
            accountManager = AccountHandler(request,interfaces,bcrypt)
            return accountManager.listSubmissionsByCurrentUser()
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
            return JsonResponse.error(exc,exc.status)
        finally:
            interfaces.close()

    @app.route("/v1/set_password/", methods=["POST"])
    @permissions_check
    def set_password():
        """ Set a new password for specified user """
        interfaces = InterfaceHolder()
        try:
            accountManager = AccountHandler(request,interfaces,bcrypt)
            return accountManager.setNewPassword()
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
            return JsonResponse.error(exc,exc.status)
        finally:
            interfaces.close()
