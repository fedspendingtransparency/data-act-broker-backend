from flask import g

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import CGAC
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode


# Add the file submission route
def add_domain_routes(app):
    """ Create routes related to domain values for flask app """

    @app.route("/v1/list_agencies/", methods=["GET"])
    def list_agencies():
        """ List all CGAC Agencies """
        sess = GlobalDB.db().session

        if g.user is None:
            cgacs = []
        elif g.user.website_admin:
            cgacs = sess.query(CGAC).all()
        else:
            cgac_ids = [affil.cgac_id for affil in g.user.affiliations]
            cgacs = sess.query(CGAC).filter(CGAC.cgac_code.in_(cgac_ids)).all()
        agency_list = [
            {'agency_name': cgac.agencyname, 'cgac_code': cgac.cgac_code}
            for cgac in cgacs
        ]
        return JsonResponse.create(StatusCode.OK,
                                   {'cgac_agency_list': agency_list})
