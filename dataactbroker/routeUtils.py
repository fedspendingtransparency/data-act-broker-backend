from dataactcore.interfaces.interfaceHolder import InterfaceHolder
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode


class RouteUtils:
    """ Holds utility functions for routes """
    SYSTEM_EMAIL = None
    CREATE_CREDENTIALS = None

    @staticmethod
    def run_instance_function(accountManager, accountFunction, *functionArgs):
        """ Standard error handling around each route """
        interfaces = InterfaceHolder()
        try:
            accountManager.addInterfaces(interfaces)
            return accountFunction(*functionArgs)
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
            return JsonResponse.error(exc,exc.status)
        finally:
            interfaces.close()