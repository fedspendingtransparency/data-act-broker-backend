from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactbroker.handlers.aws.session import LoginSession
from flask import session


class DomainHandler:

    def __init__(self, interfaces=None):
        if interfaces is not None:
            self.interfaces = interfaces
            self.validationManager = interfaces.validationDb
            self.userManager = interfaces.userDb

    def addInterfaces(self, interfaces):
        """ Add connections to databases

        Args:
            interfaces: InterfaceHolder object to DBs
        """
        self.interfaces = interfaces
        self.validationManager = interfaces.validationDb
        self.userManager = interfaces.userDb

    def listAgencies(self):
        """ Retrieves a list of all agency names and their cgac codes. If there is
         a user logged in, it will check if that user is part of the 'SYS' agency.
         If so, 'SYS' will be added to the agency_list. """
        agencies = self.validationManager.getAllAgencies()
        agency_list = []

        for agency in agencies:
            agency_list.append({"agency_name": agency.agency_name, "cgac_code": agency.cgac_code})

        if LoginSession.isLogin(session):
            user_id = LoginSession.getName(session)
            user = self.userManager.getUserByUID(user_id)
            if user.cgac_code.lower() == "sys":
                agency_list.append({"agency_name": "SYS", "cgac_code": "SYS"})

        return JsonResponse.create(StatusCode.OK, {"cgac_agency_list": agency_list})