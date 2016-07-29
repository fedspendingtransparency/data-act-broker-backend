from flask import session
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactbroker.handlers.interfaceHolder import InterfaceHolder
from dataactbroker.handlers.aws.session import LoginSession

class RouteUtils:
    """ Holds utility functions for routes """
    SYSTEM_EMAIL = None
    CREATE_CREDENTIALS = None

    @staticmethod
    def run_instance_function(accountManager, accountFunction, getSystemEmail = False, getSession = False, getUser = False, getCredentials = False, addTrueFlag = False):
        """ Standard error handling around each route """
        interfaces = InterfaceHolder()
        try:
            accountManager.addInterfaces(interfaces)
            if(getSystemEmail and getSession):
                return accountFunction(RouteUtils.SYSTEM_EMAIL,session)
            elif(getSystemEmail):
                return accountFunction(RouteUtils.SYSTEM_EMAIL)
            elif(getSession):
                return accountFunction(session)
            elif(getUser):
                if(getCredentials):
                    return accountFunction(LoginSession.getName(session),RouteUtils.CREATE_CREDENTIALS)
                else:
                    # Currently no functions with user but not credentials flag
                    raise ValueError("Invalid combination of flags to run_instance_function")
            elif(addTrueFlag):
                return accountFunction(True)
            else:
                return accountFunction()
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
            return JsonResponse.error(exc,exc.status)
        finally:
            interfaces.close()