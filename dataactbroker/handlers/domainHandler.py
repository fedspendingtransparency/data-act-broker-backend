from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import CGAC
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode

# todo: Determine if we even need this DomainHandler. It only contains one static function.

class DomainHandler:

    def list_agencies(self):
        """ Retrieves a list of all agency names and their cgac codes."""
        sess = GlobalDB.db().session
        agencies = sess.query(CGAC).all()
        agency_list = []

        for agency in agencies:
            agency_list.append({"agency_name": agency.agency_name, "cgac_code": agency.cgac_code})

        return JsonResponse.create(StatusCode.OK, {"cgac_agency_list": agency_list})
