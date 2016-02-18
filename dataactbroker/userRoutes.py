from flask import request, session
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactbroker.handlers.interfaceHolder import InterfaceHolder
from dataactbroker.permissions import permissions_check

# Add the file submission route
def add_user_routes(app):
    """ Create routes related to file submission for flask app

    """

    @app.route("/v1/register/", methods = ["POST"])
    #@permissions_check
    def register_user():
        """ Expects request to have keys for email, name, agency, and title """
        interfaces = InterfaceHolder()
        try:
            input = RequestDictionary(request)
            if(not (input.exists("email") and input.exists("name") and input.exists("agency") and input.exists("title"))):
                # Missing a required field, return 400
                exc = ResponseException("Request body must include email, name, agency, and title", StatusCode.CLIENT_ERROR)
                return JsonResponse.error(exc,exc.status,{})
            # Find user that matches specified email
            user = interfaces.userDb.getUserByEmail(input.getValue("email"))
            # Add user info to database
            interfaces.userDb.addUserInfo(user,input.getValue("name"),input.getValue("agency"),input.getValue("title"))
            # Send email to approver
            # TODO implement
            # Mark user as awaiting approval
            interfaces.userDb.changeStatus(user,"awaiting_approval")
            return JsonResponse.create(StatusCode.OK,{"message":"Registration successful"})
        except Exception as e:
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
            return JsonResponse.error(exc,exc.status,{})
        finally:
            interfaces.close()
            