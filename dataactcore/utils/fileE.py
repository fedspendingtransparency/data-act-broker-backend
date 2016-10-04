from collections import namedtuple
import logging
from operator import attrgetter

from suds.client import Client

from dataactcore.config import CONFIG_BROKER


logger = logging.getLogger(__name__)


def configValid():
    """Does the config have the necessary bits for talking to the SAM SOAP
    API"""
    sam = CONFIG_BROKER.get('sam') or {}
    hasWsdl = bool(sam.get('wsdl'))
    hasUser = bool(sam.get('username'))
    hasPass = bool(sam.get('password'))
    return hasWsdl and hasUser and hasPass


def createAuth(client):
    auth = client.factory.create('userAuthenticationKeyType')
    auth.userID = CONFIG_BROKER['sam']['username']
    auth.password = CONFIG_BROKER['sam']['password']
    return auth


def createSearch(client, dunsList):
    search = client.factory.create('entitySearchCriteriaType')
    search.DUNSList = client.factory.create('DUNSList')
    search.DUNSList.DUNSNumber = dunsList
    return search


def getEntities(client, dunsList):
    """Hit the SAM SOAP API, searching for the provided DUNS numbers. Return
    the results as a list of Suds objects"""
    params = client.factory.create('requestedData')
    params.coreData.value = 'Y'

    result = client.service.getEntities(
        createAuth(client), createSearch(client, dunsList), params)

    if result.listOfEntities:
        return result.listOfEntities.entity
    else:
        return []


Row = namedtuple('Row', (
    'AwardeeOrRecipientUniqueIdentifier',
    'UltimateParentUniqueIdentifier',
    'UltimateParentLegalEntityName',
    'HighCompOfficer1Name',
    'HighCompOfficer1Amount',
    'HighCompOfficer2Name',
    'HighCompOfficer2Amount',
    'HighCompOfficer3Name',
    'HighCompOfficer3Amount',
    'HighCompOfficer4Name',
    'HighCompOfficer4Amount',
    'HighCompOfficer5Name',
    'HighCompOfficer5Amount'))


def sudsToRow(sudsObj):
    """Convert a Suds result object into a Row tuple. This accounts for the
    presence/absence of top-paid officers"""
    comp = sudsObj.coreData.listOfExecutiveCompensationInformation
    officers = []
    officer_data = getattr(comp, 'executiveCompensationDetail', [])
    officer_data = sorted(officer_data, key=attrgetter('compensation'),
                          reverse=True)
    for data in officer_data:
        officers.append(data.name)
        officers.append(data.compensation)

    # Only top 5
    officers = officers[:10]
    # Fill in any blanks
    officers.extend([''] * (10 - len(officers)))

    return Row(
        sudsObj.entityIdentification.DUNS,
        sudsObj.coreData.DUNSInformation.globalParentDUNS.DUNSNumber,
        sudsObj.coreData.DUNSInformation.globalParentDUNS.legalBusinessName,
        *officers
    )


def retrieveRows(dunsList):
    """Soup-to-nuts creates a list of Row tuples from a set of DUNS
    numbers."""
    if configValid():
        client = Client(CONFIG_BROKER['sam']['wsdl'])
        return [sudsToRow(e) for e in getEntities(client, dunsList)]
    else:
        logger.error("Invalid sam config")
        return []
