from dataactbroker.handlers.domainHandler import DomainHandler
from dataactbroker.routeUtils import RouteUtils


# Add the file submission route
def add_domain_routes(app,isLocal,bcrypt):
    """ Create routes related to domain values for flask app

    """

    @app.route("/v1/list_agencies/", methods = ["GET"])
    def list_agencies():
        """ List all CGAC Agencies """
        domainHandler = DomainHandler()
        return RouteUtils.run_instance_function(domainHandler, domainHandler.listAgencies)
