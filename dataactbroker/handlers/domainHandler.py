from flask import session

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import CGAC
from dataactcore.models.userModel import User
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactbroker.handlers.aws.session import LoginSession

# todo: Determine if we even need this DomainHandler. It only contains one static function.

class DomainHandler:

    def list_agencies(self):
        """ Retrieves a list of all agency names and their cgac codes. If there is
         a user logged in, it will check if that user is part of the 'SYS' agency.
         If so, 'SYS' will be added to the agency_list. """
        sess = GlobalDB.db().session
        agencies = sess.query(CGAC).all()
        agency_list = []

        for agency in agencies:
            agency_list.append({"agency_name": agency.agency_name, "cgac_code": agency.cgac_code})

        if LoginSession.isLogin(session):
            user = sess.query(User).filter(User.user_id == LoginSession.getName(session)).one()
            if user.cgac_code.lower() == "sys":
                agency_list.append({"agency_name": "SYS", "cgac_code": "SYS"})

        return JsonResponse.create(StatusCode.OK, {"cgac_agency_list": agency_list})