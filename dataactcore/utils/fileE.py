from collections import namedtuple
import logging
from operator import attrgetter

from suds.client import Client

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.validationModels import FileColumn
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.lookups import FILE_TYPE_DICT


logger = logging.getLogger(__name__)


def config_valid():
    """Does the config have the necessary bits for talking to the SAM SOAP
    API"""
    sam = CONFIG_BROKER.get('sam') or {}
    has_wsdl = bool(sam.get('wsdl'))
    has_user = bool(sam.get('username'))
    has_pass = bool(sam.get('password'))
    return has_wsdl and has_user and has_pass


def create_auth(client):
    auth = client.factory.create('userAuthenticationKeyType')
    auth.userID = CONFIG_BROKER['sam']['username']
    auth.password = CONFIG_BROKER['sam']['password']
    return auth


def create_search(client, duns_list):
    search = client.factory.create('entitySearchCriteriaType')
    search.DUNSList = client.factory.create('DUNSList')
    search.DUNSList.DUNSNumber = duns_list
    return search


def get_entities(client, duns_list):
    """Hit the SAM SOAP API, searching for the provided DUNS numbers. Return
    the results as a list of Suds objects"""
    params = client.factory.create('requestedData')
    params.coreData.value = 'Y'

    result = client.service.getEntities(create_auth(client), create_search(client, duns_list), params)

    if result.transactionInformation.transactionMessage:
        logger.warning("Message from SAM API: %s", result.transactionInformation.transactionMessage)

    if result.listOfEntities:
        return result.listOfEntities.entity
    else:
        return []


Row = namedtuple('Row', (
    'AwardeeOrRecipientUniqueIdentifier',
    'UltimateParentUniqueIdentifier',
    'UltimateParentLegalEntityName',
    'HighCompOfficer1FullName',
    'HighCompOfficer1Amount',
    'HighCompOfficer2FullName',
    'HighCompOfficer2Amount',
    'HighCompOfficer3FullName',
    'HighCompOfficer3Amount',
    'HighCompOfficer4FullName',
    'HighCompOfficer4Amount',
    'HighCompOfficer5FullName',
    'HighCompOfficer5Amount'))


def suds_to_row(suds_obj):
    """Convert a Suds result object into a Row tuple. This accounts for the
    presence/absence of top-paid officers"""
    comp = getattr(suds_obj.coreData, 'listOfExecutiveCompensationInformation',
                   '')
    officers = []
    officer_data = getattr(comp, 'executiveCompensationDetail', [])
    officer_data = sorted(officer_data, key=attrgetter('compensation'),
                          reverse=True)
    for data in officer_data:
        officers.append(getattr(data, 'name', ''))
        officers.append(getattr(data, 'compensation', ''))

    # Only top 5
    officers = officers[:10]
    # Fill in any blanks
    officers.extend([''] * (10 - len(officers)))

    return Row(
        suds_obj.entityIdentification.DUNS,
        suds_obj.coreData.DUNSInformation.globalParentDUNS.DUNSNumber,
        suds_obj.coreData.DUNSInformation.globalParentDUNS.legalBusinessName,
        *officers
    )


def retrieve_rows(duns_list):
    """Soup-to-nuts creates a list of Row tuples from a set of DUNS
    numbers."""
    if config_valid():
        client = Client(CONFIG_BROKER['sam']['wsdl'])
        return [suds_to_row(e) for e in get_entities(client, duns_list)]
    else:
        logger.error("Invalid sam config")
        return []


def row_to_dict(row):
    sess = GlobalDB.db().session
    col_names = sess.query(FileColumn.name, FileColumn.name_short).\
        filter(FileColumn.file_id == FILE_TYPE_DICT['executive_compensation']).all()
    long_to_short_dict = {row.name: row.name_short for row in col_names}
    row_dict = {}

    for field in row._fields:
        key = long_to_short_dict[field.lower()]
        value = getattr(row, field)
        row_dict[key] = value if not value else str(value)
    return row_dict
