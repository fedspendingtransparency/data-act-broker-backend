from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode


class DomainHandler:

    def __init__(self, interfaces=None):
        if interfaces is not None:
            self.interfaces = interfaces
            self.validationManager = interfaces.validationDb

    def addInterfaces(self, interfaces):
        """ Add connections to databases

        Args:
            interfaces: InterfaceHolder object to DBs
        """
        self.interfaces = interfaces
        self.validationManager = interfaces.validationDb

    def listAgencies(self):
        agencies = self.validationManager.getAllAgencies()
        agency_list = []

        for agency in agencies:
            agency_list.append({"agency_name": agency.agency_name, "cgac_code": agency.cgac_code})

        return JsonResponse.create(StatusCode.OK, {"cgac_agency_list": agency_list})