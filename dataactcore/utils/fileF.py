from collections import namedtuple, OrderedDict
import itertools

import iso3166

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.fsrs import FSRSGrant, FSRSProcurement, FSRSSubcontract, FSRSSubgrant
from dataactcore.models.stagingModels import AwardFinancialAssistance, AwardProcurement
from dataactbroker.helpers.generic_helper import fy

import logging


logger = logging.getLogger(__name__)


def _country_name(code):
    """Convert a country code to the country name; return None if invalid"""
    country = iso3166.countries.get(code, None)
    if country:
        return country.name


def _zipcode_guard(model, field_prefix, match_usa):
    """Get the zip code or not depending on country value"""
    is_usa = getattr(model, field_prefix + '_country') == 'USA'
    zipcode = getattr(model, field_prefix + '_zip')
    if (match_usa and is_usa) or (not match_usa and not is_usa):
        return zipcode


def _extract_naics(naics, type):
    naics_list = naics.split(';')
    if type == 'numbers':
        return [naics_entry.split(' ')[0] for naics_entry in naics_list].join(',')
    elif type == 'titles':
        return [naics_entry.split(' ')[1:].join() for naics_entry in naics_list].join(',')


class CopyValues:
    """Copy a field value from one of our existing models"""
    # Order to check fields
    MODEL_TYPES = ('subcontract', 'subgrant', 'procurement', 'grant', 'award')

    def __init__(self, subcontract=None, subgrant=None, procurement=None, grant=None, award=None):
        self.procurement_field = procurement
        self.subcontract_field = subcontract
        self.grant_field = grant
        self.subgrant_field = subgrant
        self.award_field = award

    def __call__(self, models):
        for model_type in self.MODEL_TYPES:
            field_name = getattr(self, model_type + '_field')
            model = getattr(models, model_type)
            if model and field_name:
                return getattr(model, field_name)


def copy_subaward_field(field_name):
    return CopyValues(field_name, field_name)


def copy_prime_field(field_name):
    return CopyValues(procurement=field_name, grant=field_name)

class CopyLogic:
    """ Perform custom logic relating to the award or subaward. Instantiated with two to four functions: one for
    contracts and one for grants, or one for subcontracts, one for subgrants """
    def __init__(self, contract_fn=None, grant_fn=None, subcontract_fn=None, subgrant_fn=None):
        self.contract_fn = contract_fn
        self.grant_fn = grant_fn
        self.subcontract_fn = subcontract_fn
        self.subgrant_fn = subgrant_fn

    def __call__(self, models):
        if models.contract:
            return self.contract_fn(models.contract)
        elif models.grant:
            return self.subgrant_fn(models.grant)
        elif models.subcontract:
            return self.subcontract_fn(models.subcontract)
        elif models.subgrant:
            return self.subgrant_fn(models.subgrant)


# Collect the models associated with a single F CSV row
ModelRow = namedtuple(
    'ModelRow',
    ['award', 'procurement', 'subcontract', 'grant', 'subgrant', 'naics_desc'])
ModelRow.__new__.__defaults__ = (None, None, None, None, None)


# A collection of mappers (callables which convert a ModelRow into a string to
# be placed in a CSV cell), keyed by the CSV column name for that cell. Order
# matters as it defines the CSV column order
mappings = OrderedDict([
    # Prime Award Properties
    ('PrimeAwardUniqueKey', CopyValues('detached_award_proc_unique', 'afa_generated_unique')), # ADDED # TODO: figure this out
    ('PrimeAwardReportID', CopyValues(procurement='contract_number', grant='fain')), # RENAMED
    ('ParentAwardId', CopyValues(procurement='idv_reference_number')),
    ('PrimeAwardAmount', CopyValues(procurement='dollars_obligated', grant='total_fed_funding_amount')), # ADDED
    ('ActionDate', CopyValues(procurement='date_signed', grant='obligation_date')), # ADDED
    ('PrimeAwardFiscalYear', CopyLogic(
        contract_fn=lambda contract: fy(contract.date_signed),
        grant_fn=lambda grant: fy(grant.obligation_date)
    )), # ADDED
    ('AwardingAgencyCode', CopyValues(award='awarding_agency_code')),  # ADDED
    ('AwardingAgencyName', CopyValues(award='awarding_agency_name')),  # ADDED
    ('AwardingSubTierAgencyCode', CopyValues(procurement='contracting_office_aid', grant='federal_agency_id')),  # ADDED # TODO: figure this out
    ('AwardingSubTierAgencyName', CopyValues(procurement='contracting_office_aname', grant='federal_agency_name')),  # ADDED # TODO: figure this out
    ('AwardingOfficeCode', CopyValues(award='awarding_office_code', procurement='contracting_office_id')),  # ADDED # TODO: double check this will work
    ('AwardingOfficeName', CopyValues(award='awarding_office_name', procurement='contracting_office_name')),  # ADDED # TODO double check this will work
    ('FundingAgencyCode', CopyValues(award='awarding_office_code')),  # ADDED
    ('FundingAgencyName', CopyValues(award='awarding_office_name')),  # ADDED
    ('FundingSubTierAgencyCode', CopyValues(procurement='funding_agency_id', grant='federal_agency_id')),  # ADDED # TODO: figure this out
    ('FundingSubTierAgencyName', CopyValues(procurement='funding_agency_name', grant='federal_agency_name')),  # ADDED # TODO: figure this out
    ('FundingOfficeCode', CopyValues(award='funding_office_code', procurement='funding_office_id')),  # ADDED # TODO: double check this will work
    ('FundingOfficeName', CopyValues(award='funding_office_name', procurement='funding_office_name')),  # ADDED # TODO: double check this will work
    ('AwardeeOrRecipientUniqueIdentifier', copy_prime_field('duns')),
    ('AwardeeOrRecipientLegalEntityName', CopyValues(procurement='company_name', grant='awardee_name')), # ADDED
    ('Vendor Doing As Business Name', copy_prime_field('dba_name')),
    ('UltimateParentUniqueIdentifier', copy_prime_field('parent_duns')), # ADDED
    ('UltimateParentLegalEntityName', CopyValues(procurement='parent_company_name', grant='')), # ADDED # TODO: figure this outs
    ('LegalEntityCountryCode', CopyValues(procurement='company_address_country', grant='awardee_address_country')), # ADDED
    ('LegalEntityCountryName', CopyLogic(
        contract_fn=lambda contract: _country_name(contract.company_address_country),
        grant_fn=lambda grant: _country_name(grant.awardee_address_country)
    )), # ADDED
    ('LegalEntityAddressLine1', CopyValues(procurement='company_address_street', grant='awardee_address_street')), # ADDED
    ('LegalEntityCityName', CopyValues(procurement='company_address_city', grant='awardee_address_city')), # ADDED
    ('LegalEntityStateCode', CopyValues(procurement='company_address_state', grant='awardee_address_state')), # ADDED
    ('LegalEntityStateName', CopyValues(procurement='company_address_state_name', grant='awardee_address_state_name')), # ADDED
    ('LegalEntityZIP+4', CopyLogic(
        contract_fn=lambda contract: _zipcode_guard(contract, 'company_address', True),
        grant_fn=lambda grant: _zipcode_guard(grant, 'awardee_address', True)
    )), # ADDED
    ('LegalEntityCongressionalDistrict', CopyValues(procurement='company_address_district', grant='awardee_address_district')), # ADDED
    ('LegalEntityForeignPostalCode', CopyLogic(
        contract_fn=lambda contract: _zipcode_guard(contract, 'company_address', False),
        grant_fn=lambda grant: _zipcode_guard(grant, 'awardee_address', False)
    )), # ADDED
    ('PrimeAwardeeBusinessTypes', CopyValues(procurement='bus_types')), # ADDED
    ('PrimaryPlaceOfPerformanceCityName', copy_prime_field('principle_place_city')), # ADDED
    ('PrimaryPlaceOfPerformanceStateCode', copy_prime_field('principle_place_state')), # ADDED
    ('PrimaryPlaceOfPerformanceStateName', copy_prime_field('principle_place_state_name')), # ADDED
    ('PrimaryPlaceOfPerformanceZIP+4', copy_prime_field('principle_place_zip')), # ADDED
    ('PrimaryPlaceOfPerformanceCongressionalDistrict', copy_prime_field('principle_place_district')), # ADDED
    ('PrimaryPlaceOfPerformanceCountryCode', copy_prime_field('principle_place_country')), # ADDED
    ('PrimaryPlaceOfPerformanceCountryName', CopyLogic(
        contract_fn=lambda contract: _country_name(contract.principle_place_country),
        grant_fn=lambda grant: _country_name(grant.principle_place_country)
    )), # ADDED
    ('AwardDescription', CopyValues(award='award_description', grant='project_description')), # ADDED # TODO: double check this will work
    ('NAICS', CopyValues(procurement='naics')), # ADDED
    ('NAICS_Description', lambda models: models.naics_desc), # ADDED TODO: double check this will work
    # ('CFDA_Numbers', CopyValues(grant='cfda_numbers')), # REMOVED
    ('CFDA_Numbers', CopyLogic(
        grant_fn=lambda grant: _extract_naics(grant.cfda_numbers, 'numbers')
    )), # ADDED # TODO: double check this will work
    ('CFDA_Titles', CopyLogic(
        grant_fn=lambda grant: _extract_naics(grant.cfda_numbers, 'titles')
    )), # ADDED # TODO: double check this will work

    # Sub-Award Properties
    ('SubAwardType', lambda model: model), # ADDED # TODO: figure this out
    ('SubAwardReportYear', copy_prime_field('report_period_year')), # ADDED # TODO: ask if prime or sub
    ('SubAwardReportMonth', copy_prime_field('report_period_mon')), # ADDED # TODO: ask if prime or sub
    ('SubawardNumber', CopyValues('subcontract_num', 'subaward_num')),
    ('SubAwardAmount', CopyValues('subcontract_amount', 'subaward_amount')), # ADDED
    ('SubAwardActionDate', CopyValues('subcontract_date', 'subaward_date')), # RENAMED
    ('SubAwardeeOrRecipientUniqueIdentifier', copy_subaward_field('duns')),
    ('SubAwardeeOrRecipientLegalEntityName', CopyValues('company_name', 'awardee_name')),
    ('SubAwardeeDoingBusinessAsName', copy_subaward_field('dba_name')),
    ('SubAwardeeUltimateParentUniqueIdentifier', copy_subaward_field('parent_duns')),
    ('SubAwardeeUltimateParentLegalEntityName', CopyValues(subcontract='parent_company_name')), # TODO: figure this out
    ('SubAwardeeLegalEntityCountryCode', CopyValues('company_address_country', 'awardee_address_country')),
    ('SubAwardeeLegalEntityCountryName', CopyLogic(
        subcontract_fn=lambda subcontract: _country_name(subcontract.company_address_country),
        subgrant_fn=lambda subgrant: _country_name(subgrant.awardee_address_country)
    )),
    ('SubAwardeeLegalEntityAddressLine1', CopyValues('company_address_street', 'awardee_address_street')),
    ('SubAwardeeLegalEntityCityName', CopyValues('company_address_city', 'awardee_address_city')),
    ('SubAwardeeLegalEntityStateCode', CopyValues('company_address_state', 'awardee_address_state')),
    ('SubAwardeeLegalEntityStateName', CopyValues('company_address_state_name', 'awardee_address_state_name')),
    ('SubAwardeeLegalEntityZIP+4', CopyLogic(
        subcontract_fn=lambda subcontract: _zipcode_guard(subcontract, 'company_address', True),
        subgrant_fn=lambda subgrant: _zipcode_guard(subgrant, 'awardee_address', True)
    )),
    ('SubAwardeeLegalEntityCongressionalDistrict', CopyValues('company_address_district', 'awardee_address_district')),
    ('SubAwardeeLegalEntityForeignPostalCode', CopyLogic(
        subcontract_fn=lambda subcontract: _zipcode_guard(subcontract, 'company_address', False),
        subgrant_fn=lambda subgrant: _zipcode_guard(subgrant, 'awardee_address', False)
    )),
    ('SubAwardeeBusinessTypes', CopyValues(subcontract='bus_types')),
    ('SubAwardeePlaceOfPerformanceCityName', copy_subaward_field('principle_place_city')),
    ('SubAwardeePlaceOfPerformanceStateCode', copy_subaward_field('principle_place_state')),
    ('SubAwardeePlaceOfPerformanceStateName', copy_subaward_field('principle_place_state_name')),
    ('SubAwardeePlaceOfPerformanceZIP+4', copy_subaward_field('principle_place_zip')),
    ('SubAwardeePlaceOfPerformanceCongressionalDistrict', copy_subaward_field('principle_place_district')),
    ('SubAwardeePlaceOfPerformanceCountryCode', copy_subaward_field('principle_place_country')),
    ('SubAwardeePlaceOfPerformanceCountryName', CopyLogic(
        subcontract_fn=lambda subcontract: _country_name(subcontract.principle_place_country),
        subgrant_fn=lambda subgrant: _country_name(subgrant.principle_place_country)
    )),
    ('SubAwardDescription', CopyValues('overall_description', 'project_description')),
    ('SubAwardeeHighCompOfficer1FullName', copy_subaward_field('top_paid_fullname_1')),
    ('SubAwardeeHighCompOfficer1Amount', copy_subaward_field('top_paid_amount_1')),
    ('SubAwardeeHighCompOfficer2FullName', copy_subaward_field('top_paid_fullname_2')),
    ('SubAwardeeHighCompOfficer2Amount', copy_subaward_field('top_paid_amount_2')),
    ('SubAwardeeHighCompOfficer3FullName', copy_subaward_field('top_paid_fullname_3')),
    ('SubAwardeeHighCompOfficer3Amount', copy_subaward_field('top_paid_amount_3')),
    ('SubAwardeeHighCompOfficer4FullName', copy_subaward_field('top_paid_fullname_4')),
    ('SubAwardeeHighCompOfficer4Amount', copy_subaward_field('top_paid_amount_4')),
    ('SubAwardeeHighCompOfficer5FullName', copy_subaward_field('top_paid_fullname_5')),
    ('SubAwardeeHighCompOfficer5Amount', copy_subaward_field('top_paid_amount_5')),

    # ('PrimaryPlaceOfPerformanceAddressLine1', copy_prime_field('principle_place_street')), # REMOVED
    # ('SubAwardeePlaceOfPerformanceAddressLine1', copy_subaward_field('principle_place_street')), # REMOVED
    # ('SubcontractAwardAmount', CopyValues(subcontract='subcontract_amount')), # REMOVED
    # ('TotalFundingAmount', CopyValues(subgrant='subaward_amount')), # REMOVED
    # ('NAICS', CopyValues(subcontract='naics')), # REMOVED
    # ('NAICS_Description', lambda models: models.naics_desc), # REMOVED
    # ('CFDA_NumberAndTitle', CopyValues(subgrant='cfda_numbers')), # REMOVED
    # ('AwardingSubTierAgencyName', copy_subaward_field('funding_agency_name')), # REMOVED
    # ('AwardingSubTierAgencyCode', copy_subaward_field('funding_agency_id')), # REMOVED
    # ('AwardReportYear', copy_prime_field('report_period_year')), # REMOVED
    # ('AwardReportMonth', copy_prime_field('report_period_mon')), # REMOVED
    # ('RecModelQuestion1', CopyValues('recovery_model_q1', 'compensation_q1')), # REMOVED
    # ('RecModelQuestion2', CopyValues('recovery_model_q2', 'compensation_q2')) # REMOVED
])


def submission_procurements(submission_id):
    """ Fetch procurements and subcontracts """
    sess = GlobalDB.db().session
    log_data = {
        'message': 'Starting file F submission procurements',
        'message_type': 'CoreDebug',
        'submission_id': submission_id,
        'file_type': 'F'
    }
    logger.debug(log_data)

    award_proc_sub = sess.query(AwardProcurement.piid, AwardProcurement.parent_award_id,
                                AwardProcurement.naics_description, AwardProcurement.awarding_sub_tier_agency_c,
                                AwardProcurement.submission_id).\
        filter(AwardProcurement.submission_id == submission_id).distinct().cte("award_proc_sub")

    results = sess.query(award_proc_sub, FSRSProcurement, FSRSSubcontract).\
        filter(FSRSProcurement.contract_number == award_proc_sub.c.piid).\
        filter(FSRSProcurement.idv_reference_number.isnot_distinct_from(award_proc_sub.c.parent_award_id)).\
        filter(FSRSProcurement.contracting_office_aid == award_proc_sub.c.awarding_sub_tier_agency_c).\
        filter(FSRSSubcontract.parent_id == FSRSProcurement.id)

    # The cte returns a set of columns, not an AwardProcurement object, so we have to unpack each column
    for award_piid, award_parent_id, award_naics_desc, award_sub_tier, award_sub_id, proc, sub in results:
        # need to combine those columns again here so we can get a proper ModelRow
        award = AwardProcurement(piid=award_piid, parent_award_id=award_parent_id, naics_description=award_naics_desc,
                                 awarding_sub_tier_agency_c=award_sub_tier, submission_id=award_sub_id)
        yield ModelRow(award, proc, sub, naics_desc=award.naics_description)

    log_data['message'] = 'Finished file F submission procurements'
    logger.debug(log_data)


def submission_grants(submission_id):
    """ Fetch grants and subgrants """
    sess = GlobalDB.db().session
    log_data = {
        'message': 'Starting file F submission grants',
        'message_type': 'CoreDebug',
        'submission_id': submission_id,
        'file_type': 'F'
    }
    logger.debug(log_data)

    afa_sub = sess.query(AwardFinancialAssistance.fain, AwardFinancialAssistance.submission_id).\
        filter(AwardFinancialAssistance.submission_id == submission_id).distinct().cte("afa_sub")

    triplets = sess.query(afa_sub, FSRSGrant, FSRSSubgrant).\
        filter(FSRSGrant.fain == afa_sub.c.fain).\
        filter(FSRSSubgrant.parent_id == FSRSGrant.id)

    # The cte returns a set of columns, not an AwardFinancialAssistance object, so we have to unpack each column
    for afa_sub_fain, afa_sub_id, grant, sub in triplets:
        # need to combine those columns again here so we can get a proper ModelRow
        award = AwardFinancialAssistance(fain=afa_sub_fain, submission_id=afa_sub_id)
        yield ModelRow(award, grant=grant, subgrant=sub)

    log_data['message'] = 'Finished file F submission grants'
    logger.debug(log_data)


def generate_f_rows(submission_id):
    """ Generated OrderedDicts representing File F rows. Subawards are filtered
    to those relevant to a particular submissionId """
    log_data = {
        'message': 'Starting to generate_f_rows',
        'message_type': 'CoreDebug',
        'submission_id': submission_id,
        'file_type': 'F'
    }
    logger.debug(log_data)

    row_num = 1
    log_block_length = 1000
    for model_row in itertools.chain(submission_procurements(submission_id),
                                     submission_grants(submission_id)):
        result = OrderedDict()
        for key, mapper in mappings.items():
            value = mapper(model_row)
            if value is None:
                result[key] = ''
            else:
                result[key] = str(value)
        yield result
        if row_num % log_block_length == 0:
            log_data['message'] = 'Generated rows {}-{}'.format(row_num-(log_block_length-1), row_num)
            logger.debug(log_data)
        row_num += 1

    log_data['message'] = 'Finished generate_f_rows'
    logger.debug(log_data)
