import logging
import unicodedata
from urllib.parse import urlparse

from suds import sudsobject
from suds.client import Client
from suds.plugin import MessagePlugin
from suds.transport.https import HttpAuthenticated
from suds.xsd import doctor

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.fsrs import FSRSProcurement, FSRSSubcontract, FSRSGrant, FSRSSubgrant
from dataactcore.models.domainModels import States

logger = logging.getLogger(__name__)
PROCUREMENT = 'procurement_service'
GRANT = 'grant_service'
SERVICE_MODEL = {PROCUREMENT: FSRSProcurement, GRANT: FSRSGrant}
g_state_by_code = {}


def service_config(service_type):
    """We use or {} instead of get(key, {}) as an empty config is converted
    into None rather than an empty dict"""
    fsrs_config = CONFIG_BROKER.get('fsrs') or {}
    return fsrs_config.get(service_type) or {}


def config_valid():
    proc_wsdl = service_config(PROCUREMENT).get('wsdl')
    grant_wsdl = service_config(GRANT).get('wsdl')
    return bool(proc_wsdl) and bool(grant_wsdl)


def config_state_mappings(sess=None, init=False):
    """ Creates dictionary that maps state code to state name, deletes mapping when done"""

    if init:
        global g_state_by_code

        states = sess.query(States).all()

        for state in states:
            g_state_by_code[state.state_code] = state.state_name

        del states
    else:
        # Deletes global variable
        del g_state_by_code


class ControlFilter(MessagePlugin):
    """Suds (apparently) doesn't know how to decode certain control characters
    like ^V (synchronous idle) and ^A (start of heading). As we don't really
    care about these characters, swap them out for spaces. MessagePlugins are
    Suds's mechanism to transform SOAP content before it gets parsed."""
    @staticmethod
    def is_control(char):
        """Unicode has a several "categories" related to "control" characters;
        all of the categories begin with 'C'. Note that newlines _are_ a
        control character; we're banking on this swap for spaces not being
        super important.
        http://www.unicode.org/reports/tr44/#GC_Values_Table
        """
        return unicodedata.category(char).startswith('C')

    def received(self, context):
        """Overrides this method in MessagePlugin to replace control
        characters with spaces"""
        with_controls = context.reply.decode('UTF-8')
        without_controls = ''.join(
            char if not self.is_control(char) else ' '
            for char in with_controls
        )
        context.reply = without_controls.encode('UTF-8')


class ZeroDateFilter(MessagePlugin):
    """Suds will automatically convert date/datetime fields into their
    corresponding Python type (yay). This places an implicit constraint,
    though, in that the dates need to pass Python requirements (such as being
    having a year between 1 and 9999, month between 1 and 12, etc.). Account
    for 0000-00-00 by swapping it for the similarly nonsensical (but
    parseable) 0001-01-01"""
    def received(self, context):
        """Overrides this method in MessagePlugin"""
        original = context.reply.decode('UTF-8')
        modified = original.replace('0000-00-00', '0001-01-01')
        context.reply = modified.encode('UTF-8')


def new_client(service_type):
    """Make a `suds` client, accounting for ?wsdl suffixes, failing to import
    appropriate schemas, and http auth"""
    config = service_config(service_type)
    wsdl_url = config.get('wsdl', '')
    options = {'url': wsdl_url}

    if wsdl_url.endswith('?wsdl'):
        options['location'] = wsdl_url[:-len('?wsdl')]

    # The WSDL is missing an import; it's so common that suds has a work around
    parsed_wsdl = urlparse(wsdl_url)
    import_fix = doctor.Import('http://schemas.xmlsoap.org/soap/encoding/')
    # Main namespace is the wsdl domain
    import_fix.filter.add('{}://{}/'.format(parsed_wsdl.scheme, parsed_wsdl.netloc))

    options['doctor'] = doctor.ImportDoctor(import_fix)
    options['plugins'] = [ControlFilter(), ZeroDateFilter()]

    if config.get('username') and config.get('password'):
        options['transport'] = HttpAuthenticated(
            username=config['username'],
            password=config['password'],
            timeout=300)

    return Client(**options)


def soap_to_dict(soap_obj):
    """A recursive version of sudsobject.asdict"""
    if isinstance(soap_obj, sudsobject.Object):
        return {k: soap_to_dict(v) for k, v in soap_obj}
    elif isinstance(soap_obj, list):
        return [soap_to_dict(v) for v in soap_obj]
    return soap_obj


# Fields lists to copy
_common = ('duns', 'dba_name', 'parent_duns', 'funding_agency_id', 'funding_agency_name')
_contract = ('company_name', 'parent_company_name', 'naics', 'funding_office_id', 'funding_office_name',
             'recovery_model_q1', 'recovery_model_q2')
_grant = ('dunsplus4', 'awardee_name', 'project_description', 'compensation_q1', 'compensation_q2', 'federal_agency_id',
          'federal_agency_name')
_prime = ('internal_id', 'date_submitted', 'report_period_mon', 'report_period_year')
_primeContract = _common + _contract + _prime + (
    'id', 'contract_number', 'idv_reference_number', 'report_type', 'contract_agency_code', 'contract_idv_agency_code',
    'contracting_office_aid', 'contracting_office_aname', 'contracting_office_id', 'contracting_office_name',
    'treasury_symbol', 'dollar_obligated', 'date_signed', 'transaction_type', 'program_title')
_subContract = _common + _contract + (
    'subcontract_amount', 'subcontract_date', 'subcontract_num', 'overall_description', 'recovery_subcontract_amt')
_primeGrant = _common + _grant + _prime + ('id', 'fain', 'total_fed_funding_amount', 'obligation_date')
_subGrant = _common + _grant + ('subaward_amount', 'subaward_date', 'subaward_num')
# Address fields
_contractAddrs = ('principle_place', 'company_address')
_grantAddrs = ('principle_place', 'awardee_address')


def flatten_soap_dict(simple_fields, address_fields, comma_field, soap_dict):
    """For all four FSRS models, we need to copy over values, flatten address
    data, flatten topPaid, convert comma fields"""
    model_attrs = {}
    for field in simple_fields:
        model_attrs[field] = soap_dict.get(field)
    for prefix in address_fields:
        for field in ('city', 'street', 'state', 'country', 'zip', 'district'):
            model_attrs[prefix + '_' + field] = soap_dict[prefix].get(field)

            # Deriving state name since not provided by FSRS feed
            if field == 'state':
                # Only populate for USA locations
                if soap_dict[prefix].get('country') and soap_dict[prefix]['country'].upper() == 'USA':
                    model_attrs[prefix + '_state_name'] = g_state_by_code.get(model_attrs[prefix + '_state'])
                else:
                    model_attrs[prefix + '_state_name'] = None

    for idx in range(5):
        idx = str(idx + 1)
        if 'top_pay_employees' in soap_dict:
            info = soap_dict['top_pay_employees']['employee_' + idx]
            model_attrs['top_paid_fullname_' + idx] = info['fullname']
            model_attrs['top_paid_amount_' + idx] = info['amount']
    model_attrs[comma_field] = ','.join(soap_dict.get(comma_field, []))
    return model_attrs


def to_prime_contract(soap_dict):
    model_attrs = flatten_soap_dict(_primeContract, _contractAddrs, 'bus_types', soap_dict)
    model_attrs['subawards'] = [to_subcontract(sub) for sub in soap_dict.get('subcontractors', [])]

    debug_dict = {'id': model_attrs['id'], 'internal_id': model_attrs['internal_id'],
                  'subaward_count': len(model_attrs['subawards'])}
    logger.debug('Procurement: %s' % str(debug_dict))

    return FSRSProcurement(**model_attrs)


def to_subcontract(soap_dict):
    model_attrs = flatten_soap_dict(_subContract, _contractAddrs, 'bus_types', soap_dict)
    return FSRSSubcontract(**model_attrs)


def to_prime_grant(soap_dict):
    model_attrs = flatten_soap_dict(_primeGrant, _grantAddrs, 'cfda_numbers', soap_dict)
    model_attrs['subawards'] = [to_subgrant(sub) for sub in soap_dict.get('subawardees', [])]

    debug_dict = {'id': model_attrs['id'], 'internal_id': model_attrs['internal_id'],
                  'subaward_count': len(model_attrs['subawards'])}
    logger.debug('Grant: %s' % str(debug_dict))

    return FSRSGrant(**model_attrs)


def to_subgrant(soap_dict):
    model_attrs = flatten_soap_dict(_subGrant, _grantAddrs, 'cfda_numbers', soap_dict)
    return FSRSSubgrant(**model_attrs)


def retrieve_batch(service_type, min_id, return_single_id):
    """The FSRS web service returns records in batches (500 at a time).
    Retrieve one such batch, converting each result (and sub-results) into
    dicts"""

    # Subtracting 1 from min_id since FSRS API starts one after value
    # If the last id is 50 for example the min_id is 51, the API will retrieve 52 and greater
    for report in new_client(service_type).service.getData(id=min_id-1)['reports']:
        if (report['id'] == min_id and return_single_id) or not return_single_id:
            as_dict = soap_to_dict(report)
            if service_type == PROCUREMENT:
                yield to_prime_contract(as_dict)
            else:
                yield to_prime_grant(as_dict)


def fetch_and_replace_batch(sess, service_type, min_id=None):
    """Hit one of the FSRS APIs and replace any local records that match.
    Returns the award models"""
    model = SERVICE_MODEL[service_type]
    return_single_id = True

    if min_id is None:
        min_id = model.next_id(sess)
        return_single_id = False

    awards = list(retrieve_batch(service_type, min_id, return_single_id))
    ids = [a.internal_id for a in awards]
    sess.query(model).filter(model.internal_id.in_(ids)).delete(synchronize_session=False)
    sess.add_all(awards)
    sess.commit()

    return awards
