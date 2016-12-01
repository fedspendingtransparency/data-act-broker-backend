from dataactbroker.handlers.domainHandler import DomainHandler


# Add the file submission route
def add_domain_routes(app, isLocal, bcrypt):
    """ Create routes related to domain values for flask app

    """

    @app.route("/v1/list_agencies/", methods = ["GET"])
    def list_agencies():
        """ List all CGAC Agencies """
        domainHandler = DomainHandler()
        return domainHandler.list_agencies()
