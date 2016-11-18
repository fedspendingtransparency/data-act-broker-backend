from dataactbroker.handlers.domainHandler import DomainHandler
from dataactcore.interfaces.interfaceHolder import InterfaceHolder


# Add the file submission route
def add_domain_routes(app,isLocal,bcrypt):
    """ Create routes related to domain values for flask app

    """

    @app.route("/v1/list_agencies/", methods = ["GET"])
    def list_agencies():
        """ List all CGAC Agencies """
        domainHandler = DomainHandler()
        domainHandler.addInterfaces(InterfaceHolder())    # soon to be removed
        return domainHandler.list_agencies()
