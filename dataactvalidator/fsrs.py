import logging
from urllib.parse import urlparse

from suds import sudsobject
from suds.client import Client
from suds.transport.https import HttpAuthenticated
from suds.xsd import doctor

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.fsrs import (
    FSRSProcurement, FSRSSubcontract, FSRSGrant, FSRSSubgrant)


logger = logging.getLogger(__name__)
PROCUREMENT = 'procurement_service'
GRANT = 'grant_service'
SERVICE_MODEL = {PROCUREMENT: FSRSProcurement, GRANT: FSRSGrant}


def serviceConfig(serviceType):
    """We use or {} instead of get(key, {}) as an empty config is converted
    into None rather than an empty dict"""
    fsrsConfig = CONFIG_BROKER.get('fsrs') or {}
    return fsrsConfig.get(serviceType) or {}


def configValid():
    procWSDL = serviceConfig(PROCUREMENT).get('wsdl')
    grantWSDL = serviceConfig(GRANT).get('wsdl')
    return bool(procWSDL) and bool(grantWSDL)


def newClient(serviceType):
    """Make a `suds` client, accounting for ?wsdl suffixes, failing to import
    appropriate schemas, and http auth"""
    config = serviceConfig(serviceType)
    wsdlUrl = config.get('wsdl', '')
    options = {'url': wsdlUrl}

    if wsdlUrl.endswith('?wsdl'):
        options['location'] = wsdlUrl[:-len('?wsdl')]

    # The WSDL is missing an import; it's so common that suds has a work around
    parsedWsdl = urlparse(wsdlUrl)
    importFix = doctor.Import('http://schemas.xmlsoap.org/soap/encoding/')
    importFix.filter.add(   # Main namespace is the wsdl domain
        '{}://{}/'.format(parsedWsdl.scheme, parsedWsdl.netloc))

    options['doctor'] = doctor.ImportDoctor(importFix)

    if config.get('username') and config.get('password'):
        options['transport'] = HttpAuthenticated(
            username=config['username'],
            password=config['password'])

    return Client(**options)


def soap2Dict(soapObj):
    """A recursive version of sudsobject.asdict"""
    if isinstance(soapObj, sudsobject.Object):
        return {k: soap2Dict(v) for k, v in soapObj}
    elif isinstance(soapObj, list):
        return [soap2Dict(v) for v in soapObj]
    return soapObj


# Fields lists to copy
_common = ('duns', 'dba_name', 'parent_duns', 'funding_agency_id',
           'funding_agency_name')
_contract = ('company_name', 'parent_company_name', 'naics',
             'funding_office_id', 'funding_office_name', 'recovery_model_q1',
             'recovery_model_q2')
_grant = ('dunsplus4', 'awardee_name', 'project_description',
          'compensation_q1', 'compensation_q2')
_prime = ('internal_id', 'date_submitted', 'report_period_mon',
          'report_period_year')
_primeContract = _common + _contract + _prime + (
    'id', 'contract_number', 'idv_reference_number', 'report_type',
    'contract_agency_code', 'contract_idv_agency_code',
    'contracting_office_aid', 'contracting_office_aname',
    'contracting_office_id', 'contracting_office_name', 'treasury_symbol',
    'dollar_obligated', 'date_signed', 'transaction_type', 'program_title')
_subContract = _common + _contract + (
    'subcontract_amount', 'subcontract_date', 'subcontract_num',
    'overall_description', 'recovery_subcontract_amt')
_primeGrant = _common + _grant + _prime + (
    'id', 'fain', 'total_fed_funding_amount', 'obligation_date')
_subGrant = _common + _grant + (
    'subaward_amount', 'subaward_date', 'subaward_num')
# Address fields
_contractAddrs = ('principle_place', 'company_address')
_grantAddrs = ('principle_place', 'awardee_address')


def flattenSoapDict(simpleFields, addressFields, commaField, soapDict):
    """For all four FSRS models, we need to copy over values, flatten address
    data, flatten topPaid, convert comma fields"""
    logger.debug(soapDict)
    modelAttrs = {}
    for field in simpleFields:
        modelAttrs[field] = soapDict.get(field)
    for prefix in addressFields:
        for field in ('city', 'street', 'state', 'country', 'zip', 'district'):
            modelAttrs[prefix + '_' + field] = soapDict[prefix].get(field)
    for idx in range(5):
        idx = str(idx + 1)
        if 'top_pay_employees' in soapDict:
            info = soapDict['top_pay_employees']['employee_' + idx]
            modelAttrs['top_paid_fullname_' + idx] = info['fullname']
            modelAttrs['top_paid_amount_' + idx] = info['amount']
    modelAttrs[commaField] = ','.join(soapDict.get(commaField, []))
    return modelAttrs


def toPrimeContract(soapDict):
    modelAttrs = flattenSoapDict(
        _primeContract, _contractAddrs, 'bus_types', soapDict)
    modelAttrs['subawards'] = [
        toSubcontract(sub) for sub in soapDict['subcontractors']
    ]
    return FSRSProcurement(**modelAttrs)


def toSubcontract(soapDict):
    modelAttrs = flattenSoapDict(
        _subContract, _contractAddrs, 'bus_types', soapDict)
    return FSRSSubcontract(**modelAttrs)


def toPrimeGrant(soapDict):
    modelAttrs = flattenSoapDict(
        _primeGrant, _grantAddrs, 'cfda_numbers', soapDict)
    modelAttrs['subawards'] = [
        toSubgrant(sub) for sub in soapDict['subawardees']
    ]
    return FSRSGrant(**modelAttrs)


def toSubgrant(soapDict):
    modelAttrs = flattenSoapDict(
        _subGrant, _grantAddrs, 'cfda_numbers', soapDict)
    return FSRSSubgrant(**modelAttrs)


def retrieveBatch(serviceType, minId):
    """The FSRS web service returns records in batches (500 at a time).
    Retrieve one such batch, converting each result (and sub-results) into
    dicts"""
    for report in newClient(serviceType).service.getData(id=minId)['reports']:
        yield soap2Dict(report)


def fetchAndReplaceBatch(sess, serviceType, minId=None):
    """Hit one of the FSRS APIs and replace any local records that match.
    Returns the award models"""
    model = SERVICE_MODEL[serviceType]
    if minId is None:
        minId = model.nextId(sess)

    batch = retrieveBatch(serviceType, minId)
    if serviceType == PROCUREMENT:
        awards = [toPrimeContract(a) for a in batch]
    else:
        awards = [toPrimeGrant(a) for a in batch]

    ids = [a.id for a in awards]
    sess.query(model).filter(model.id.in_(ids)).delete(
        synchronize_session=False)
    sess.add_all(awards)
    sess.commit()

    return awards
