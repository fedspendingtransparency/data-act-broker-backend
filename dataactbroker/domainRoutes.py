from dataactbroker.handlers.validationHandler import ValidationHandler
from dataactbroker.permissions import permissions_check
from dataactbroker.routeUtils import RouteUtils

# Add the file submission route
def add_domain_routes(app,isLocal,bcrypt):
    """ Create routes related to domain values for flask app

    """

    @app.route("/v1/list_agencies/", methods = ["GET"])
    @permissions_check
    def list_agencies():
        """ List all CGAC Agencies """
        validationHandler = ValidationHandler()
        return RouteUtils.run_instance_function(validationHandler, validationHandler.listAgencies)
