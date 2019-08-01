import logging
import time
import urllib.error
import suds
import http.client
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

from dataactcore.config import CONFIG_BROKER


logger = logging.getLogger(__name__)
# this will prevent suds from printing the entire XML on errors and generating lengthy logs
logging.getLogger('suds.client').setLevel(logging.CRITICAL)


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

    retries = 0
    max_retries = 5
    while retries < max_retries:
        try:
            result = client.service.getEntities(create_auth(client), create_search(client, duns_list), params)
            break
        except (urllib.error.HTTPError, suds.TypeNotFound, http.client.IncompleteRead):
            logger.warning('SAM service might be temporarily down. Trying again in five seconds.')
            time.sleep(5)
            retries += 1
    if retries == max_retries:
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
