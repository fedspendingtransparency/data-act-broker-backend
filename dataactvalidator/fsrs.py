from urllib.parse import urlparse

from suds.client import Client
from suds.transport.https import HttpAuthenticated
from suds.xsd import doctor

from dataactcore.config import CONFIG_BROKER


def newClient():
    """Make a `suds` client, accounting for ?wsdl suffixes, failing to import
    appropriate schemas, and http auth"""
    serviceConfig = CONFIG_BROKER.get('fsrs_service', {})
    wsdlUrl = serviceConfig.get('wsdl', '')
    options = {'url': wsdlUrl}

    if wsdlUrl.endswith('?wsdl'):
        options['location'] = wsdlUrl[:-len('?wsdl')]

    # The WSDL is missing an import; it's so common that suds has a work around
    parsedWsdl = urlparse(wsdlUrl)
    importFix = doctor.Import('http://schemas.xmlsoap.org/soap/encoding/')
    importFix.filter.add(   # Main namespace is the wsdl domain
        '{}://{}/'.format(parsedWsdl.scheme, parsedWsdl.netloc))

    options['doctor'] = doctor.ImportDoctor(importFix)

    if serviceConfig.get('username') and serviceConfig.get('password'):
        options['transport'] = HttpAuthenticated(
            username=serviceConfig['username'],
            password=serviceConfig['password'])

    return Client(**options)
