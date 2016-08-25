import itertools
from urllib.parse import urlparse

from suds import sudsobject
from suds.client import Client
from suds.transport.https import HttpAuthenticated
from suds.xsd import doctor

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.fsrs import FSRSAward, FSRSSubaward


def configValid():
    serviceConfig = CONFIG_BROKER.get('fsrs_service') or {}
    return bool(serviceConfig.get('wsdl'))


def newClient():
    """Make a `suds` client, accounting for ?wsdl suffixes, failing to import
    appropriate schemas, and http auth"""
    serviceConfig = CONFIG_BROKER.get('fsrs_service') or {}
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


def soap2Dict(soapObj):
    """A recursive version of sudsobject.asdict"""
    if isinstance(soapObj, sudsobject.Object):
        return {k: soap2Dict(v) for k, v in soapObj}
    elif isinstance(soapObj, list):
        return [soap2Dict(v) for v in soapObj]
    return soapObj


def commonAttributes(soapDict):
    """Awards and SubAwards share some fields; set them up here. Accounts for
    contained locations and top-paid employees"""
    fields = (
        'duns', 'company_name', 'dba_name', 'parent_duns',
        'parent_company_name', 'naics', 'funding_agency_id',
        'funding_agency_name', 'funding_office_id', 'funding_office_name',
        'recovery_model_q1', 'recovery_model_q2'
    )
    modelAttrs = {field: soapDict.get(field) for field in fields}
    modelAttrs['bus_types'] = ','.join(soapDict['bus_types'])

    addrFields = ('city', 'street', 'state', 'country', 'zip', 'district')
    prefixes = ('company_address', 'principle_place')
    for prefix, field in itertools.product(prefixes, addrFields):
        modelAttrs[prefix + '_' + field] = soapDict[prefix].get(field)

    for idx in range(5):
        idx = str(idx + 1)
        if 'top_pay_employees' in soapDict:
            info = soapDict['top_pay_employees']['employee_' + idx]
            modelAttrs['top_paid_fullname_' + idx] = info['fullname']
            modelAttrs['top_paid_amount_' + idx] = info['amount']

    return modelAttrs


def award2Model(soapDict):
    """Convert SOAP webservice's dict into a FSRSAward ORM model, including
    referenced FSRSSubaward"""
    modelAttrs = commonAttributes(soapDict)
    fields = (
        'id', 'internal_id', 'contract_number', 'idv_reference_number',
        'date_submitted', 'report_period_mon', 'report_period_year',
        'report_type', 'contract_agency_code', 'contract_idv_agency_code',
        'contracting_office_aid', 'contracting_office_aname',
        'contracting_office_id', 'contracting_office_name', 'treasury_symbol',
        'dollar_obligated', 'date_signed', 'transaction_type', 'program_title'
    )
    for field in fields:
        modelAttrs[field] = soapDict.get(field)

    modelAttrs['subawards'] = [
        subaward2Model(sub) for sub in soapDict['subcontractors']
    ]

    return FSRSAward(**modelAttrs)


def subaward2Model(soapDict):
    """Convert SOAP webservice's dict into a FSRSSubaward ORM model"""
    modelAttrs = commonAttributes(soapDict)
    fields = (
        'subcontract_amount', 'subcontract_date', 'subcontract_num',
        'overall_description', 'recovery_subcontract_amt'
    )
    for field in fields:
        modelAttrs[field] = soapDict.get(field)

    return FSRSSubaward(**modelAttrs)


def retrieveSoapDictBatch(minId):
    """The FSRS web service returns records in batches (500 at a time).
    Retrieve one such batch, converting each result (and sub-results) into
    dicts"""
    for report in newClient().service.getData(id=minId)['reports']:
        yield soap2Dict(report)


def retrieveAwardsBatch(minId):
    """Same as retrieveSoapDictBatch but converts to FSRSAward models"""
    for soapDict in retrieveSoapDictBatch(minId):
        yield award2Model(soapDict)
