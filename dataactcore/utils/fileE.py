from collections import namedtuple
import logging
from operator import attrgetter

import urllib.error

from dataactbroker.helpers.generic_helper import get_client

from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.validationModels import FileColumn
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.lookups import FILE_TYPE_DICT


logger = logging.getLogger(__name__)


def config_valid():
    """ Does the config have the necessary bits for talking to the SAM SOAP API

        Returns:
            A boolean indicating whether the configs have a wsdl, user, and password defined
    """
    sam = CONFIG_BROKER.get('sam') or {}
    has_wsdl = bool(sam.get('wsdl'))
    has_user = bool(sam.get('username'))
    has_pass = bool(sam.get('password'))
    return has_wsdl and has_user and has_pass


def create_auth(client):
    """ Creates an authentication key for the user provided.

        Args:
            client: an object containing information about the client

        Returns:
            An object containing details of authentication for a SAM user.
    """
    auth = client.factory.create('userAuthenticationKeyType')
    auth.userID = CONFIG_BROKER['sam']['username']
    auth.password = CONFIG_BROKER['sam']['password']
    return auth


def create_search(client, duns_list):
    """ Creates a search criteria object

        Args:
            client: an object containing information about the client
            duns_list: A list of DUNS to search for

        Returns:
            An object containing details of search criteria to use for the SAM query.
    """
    search = client.factory.create('entitySearchCriteriaType')
    search.DUNSList = client.factory.create('DUNSList')
    search.DUNSList.DUNSNumber = duns_list
    return search


def get_entities(client, duns_list):
    """ Hit the SAM SOAP API, searching for the provided DUNS numbers. Return the results as a list of Suds objects.

        Args:
            client: an object containing information about the client
            duns_list: A list of DUNS to search for

        Returns:
            A list of SAM entries for the provided DUNS or an empty list if there are no results.

        Raises:
            ValueError: If SAM credentials are not valid
            ResponseException: If SAM is not responding
    """
    params = client.factory.create('requestedData')
    params.coreData.value = 'Y'

    try:
        result = client.service.getEntities(create_auth(client), create_search(client, duns_list), params)
    except urllib.error.HTTPError:
        raise ResponseException("Unable to contact SAM service, which may be experiencing downtime or intermittent "
                                "performance issues. Please try again later.", StatusCode.NOT_FOUND)

    # If result is the string "-1" then our credentials aren't correct, inform the user of this
    if result == "-1":
        raise ValueError("Invalid SAM credentials, please contact the Service Desk.")

    if result.transactionInformation.transactionMessage:
        logger.warning({
            'message': 'Message from SAM API: {}'.format(result.transactionInformation.transactionMessage),
            'message_type': 'CoreWarning'
        })

    if result.listOfEntities:
        return result.listOfEntities.entity
    else:
        return []


Row = namedtuple('Row', (
    'AwardeeOrRecipientUniqueIdentifier',
    'AwardeeOrRecipientLegalEntityName',
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
    """ Convert a Suds result object into a Row tuple. This accounts for the presence/absence of top-paid officers

        Args:
            suds_obj: a Suds object

        Returns:
            A row object containing all relevant data from the Suds object
    """
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
        suds_obj.entityIdentification.legalBusinessName,
        suds_obj.coreData.DUNSInformation.globalParentDUNS.DUNSNumber,
        suds_obj.coreData.DUNSInformation.globalParentDUNS.legalBusinessName,
        *officers
    )


def retrieve_rows(duns_list):
    """ Soup-to-nuts creates a list of Row tuples from a set of DUNS numbers.

        Args:
            duns_list: A list of DUNS to search for
    """
    if config_valid():
        return [suds_to_row(e) for e in get_entities(get_client(), duns_list)]
    else:
        logger.error({
            'message': "Invalid sam config",
            'message_type': 'CoreError'
        })
        return []


def row_to_dict(row):
    """ Converts a row into a dictionary that can be stored in the executive_compensation table.

        Args:
            row: A row of executive_compensation data
    """
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
