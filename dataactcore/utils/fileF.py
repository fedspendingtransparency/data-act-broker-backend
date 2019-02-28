from collections import namedtuple, OrderedDict
import itertools

import iso3166

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.fsrs import FSRSGrant, FSRSProcurement, FSRSSubcontract, FSRSSubgrant
from dataactcore.models.stagingModels import AwardFinancialAssistance, AwardProcurement
from dataactcore.models.domainModels import DUNS
from dataactbroker.helpers.generic_helper import fy

import logging


logger = logging.getLogger(__name__)


def _prime_unique_id(model):
    """ Given an FSRS award, derive its unique key

        Args:
            award: either AwardProcurement or AwardFinancialAssistance

        Returns:
            the concatenated award key, or None
    """
    # Note: This ID cannot be generated using submission award data (for contracts) because agency_id is not included
    #       in the AwardProcurement model. For now this field can be used to match the prime award.
    unique_key_attrs = []
    unique_fields = []
    if isinstance(model, FSRSSubgrant):
        unique_key_attrs = ['CONT_AW']
        unique_fields = ['contract_agency_code', 'contract_idv_agency_code', 'contract_number', 'idv_reference_number']
    elif isinstance(model, FSRSSubcontract):
        unique_fields = ['fain']

    for unique_field in unique_fields:
        unique_key_attrs.append(model.get(unique_field) or '-none-')
    unique_key = '_'.join(unique_key_attrs)

    return unique_key


def _determine_sub_award_type(subaward):
    """ Given an subaward, determine its type

        Args:
            subaward: either FSRSSubcontract or FSRSSubgrant

        Returns:
            a string representing the subaward type
    """
    if isinstance(subaward, FSRSSubcontract):
        return 'sub-contract'
    elif isinstance(subaward, FSRSSubgrant):
        return 'sub-grant'


def _country_name(code):
    """ Convert a country code to the country name

        Args:
            code: country code

        Returns:
            a string representing the country name, or None if invalid
    """
    country = iso3166.countries.get(code, None)
    if country:
        return country.name


def _zipcode_guard(model, field_prefix, match_usa):
    """ Get the zip code or not depending on country value

        Args:
            model: model to extract code from
            field_prefix: string prefix depending on legal entity or place of performance
            match_usa: whether the zip should be in USA or not

        Returns:
            zip code, or None if invalid
    """
    is_usa = getattr(model, field_prefix + '_country') == 'USA'
    zipcode = getattr(model, field_prefix + '_zip')
    if (match_usa and is_usa) or (not match_usa and not is_usa):
        return zipcode


def _extract_naics(naics, type):
    """ Get the naics codes or titles

        Args:
            naics: naics string to parse
            type: whether to return 'numbers' or 'titles'

        Returns:
            naics codes or titles, or None if invalid
    """
    naics_list = naics.split(';')
    if type == 'numbers':
        return [naics_entry.split(' ')[0] for naics_entry in naics_list].join(',')
    elif type == 'titles':
        return [naics_entry.split(' ')[1:].join() for naics_entry in naics_list].join(',')


def _derive_duns_name(duns):
    """ Get the duns name, if available

        Args:
            duns: duns to find

        Returns:
            duns name, or None
    """
    sess = GlobalDB.db().session

    duns = sess.query(DUNS).filter(DUNS.awardee_or_recipient_uniqu == duns).one_or_none()
    if duns:
        return duns.legal_business_name


def _derive_duns_business_types(duns):
    """ Get the duns business types, if available

        Args:
            duns: duns to find

        Returns:
            duns name, or None
    """
    sess = GlobalDB.db().session

    duns = sess.query(DUNS).filter(DUNS.awardee_or_recipient_uniqu == duns).one_or_none()
    if duns:
        return duns.business_types_codes


class DeriveValues:
    """ Derive values (either by copying or applying logic from one of our existing models """
    # Order to check fields
    MODEL_TYPES = ('subcontract', 'subgrant', 'procurement', 'grant', 'award')

    def __init__(self, subcontract=None, subgrant=None, procurement=None, grant=None, award=None, procurement_fn=None,
                 grant_fn=None, subcontract_fn=None, subgrant_fn=None, award_fn=None):
        # Copy fields
        self.procurement_field = procurement
        self.subcontract_field = subcontract
        self.grant_field = grant
        self.subgrant_field = subgrant
        self.award_field = award

        # Logic functions
        self.procurement_fn = procurement_fn
        self.subcontract_fn = subcontract_fn
        self.grant_fn = grant_fn
        self.subgrant_fn = subgrant_fn
        self.award_fn = award_fn

    def __call__(self, models):
        for model_type in self.MODEL_TYPES:
            field_name = getattr(self, model_type + '_field')
            field_fn = getattr(self, model_type + '_fn')
            model = getattr(models, model_type)
            if model and field_name:
                return getattr(model, field_name)
            elif model and field_fn:
                return field_fn(model)


def copy_subaward_field(field_name):
    """ Copy the same field from both subrants/subcntracts

        Args:
            field_name: the field name to copy from

        Returns:
            DeriveValues for both grants/contracts
    """
    return DeriveValues(field_name, field_name)


def copy_prime_field(field_name):
    """ Copy the same field from both grants/contracts

        Args:
            field_name: the field name to copy from

        Returns:
            DeriveValues for both grants/contracts
    """
    return DeriveValues(procurement=field_name, grant=field_name)


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
    ('PrimeAwardUniqueKey', DeriveValues(
        procurement_fn=lambda contract: _prime_unique_id(contract),
        grant_fn=lambda grant: _prime_unique_id(grant)
    )),
    ('PrimeAwardReportID', DeriveValues(procurement='contract_number', grant='fain')),
    ('ParentAwardId', DeriveValues(procurement='idv_reference_number')),
    ('PrimeAwardAmount', DeriveValues(procurement='dollar_obligated', grant='total_fed_funding_amount')),
    ('ActionDate', DeriveValues(procurement='date_signed', grant='obligation_date')),
    ('PrimeAwardFiscalYear', DeriveValues(
        procurement_fn=lambda contract: 'FY{}'.format(fy(contract.date_signed)) if fy(contract.date_signed) else None,
        grant_fn=lambda grant: 'FY{}'.format(fy(grant.obligation_date)) if fy(grant.obligation_date) else None
    )),
    ('AwardingAgencyCode', DeriveValues(award='awarding_agency_code')),
    ('AwardingAgencyName', DeriveValues(award='awarding_agency_name')),
    ('AwardingSubTierAgencyCode', DeriveValues(procurement='contracting_office_aid', grant='federal_agency_id')),
    ('AwardingSubTierAgencyName', DeriveValues(procurement='contracting_office_aname',
                                               award='awarding_sub_tier_agency_n')),
    ('AwardingOfficeCode', DeriveValues(procurement='contracting_office_id', award='awarding_office_code')),
    ('AwardingOfficeName', DeriveValues(procurement='contracting_office_name', award='awarding_office_name')),
    ('FundingAgencyCode', DeriveValues(award='funding_agency_code')),
    ('FundingAgencyName', DeriveValues(award='funding_agency_name')),
    ('FundingSubTierAgencyCode', DeriveValues(procurement='funding_agency_id', award='funding_sub_tier_agency_co')),
    ('FundingSubTierAgencyName', DeriveValues(procurement='funding_agency_name', award='funding_sub_tier_agency_co')),
    ('FundingOfficeCode', DeriveValues(procurement='funding_office_id', award='funding_office_code')),
    ('FundingOfficeName', DeriveValues(procurement='funding_office_name', award='funding_office_name')),
    ('AwardeeOrRecipientUniqueIdentifier', copy_prime_field('duns')),
    ('AwardeeOrRecipientLegalEntityName', DeriveValues(procurement='company_name', grant='awardee_name')),
    ('Vendor Doing As Business Name', copy_prime_field('dba_name')),
    ('UltimateParentUniqueIdentifier', copy_prime_field('parent_duns')),
    ('UltimateParentLegalEntityName', DeriveValues(
        procurement='parent_company_name',
        grant_fn=lambda grant: _derive_duns_name(grant.parent_duns)
    )),
    ('LegalEntityCountryCode', DeriveValues(procurement='company_address_country', grant='awardee_address_country')),
    ('LegalEntityCountryName', DeriveValues(
        procurement_fn=lambda contract: _country_name(contract.company_address_country),
        grant_fn=lambda grant: _country_name(grant.awardee_address_country)
    )),
    ('LegalEntityAddressLine1', DeriveValues(procurement='company_address_street', grant='awardee_address_street')),
    ('LegalEntityCityName', DeriveValues(procurement='company_address_city', grant='awardee_address_city')),
    ('LegalEntityStateCode', DeriveValues(procurement='company_address_state', grant='awardee_address_state')),
    ('LegalEntityStateName', DeriveValues(procurement='company_address_state_name',
                                          grant='awardee_address_state_name')),
    ('LegalEntityZIP+4', DeriveValues(
        procurement_fn=lambda contract: _zipcode_guard(contract, 'company_address', True),
        grant_fn=lambda grant: _zipcode_guard(grant, 'awardee_address', True)
    )),
    ('LegalEntityCongressionalDistrict', DeriveValues(procurement='company_address_district',
                                                      grant='awardee_address_district')),
    ('LegalEntityForeignPostalCode', DeriveValues(
        procurement_fn=lambda contract: _zipcode_guard(contract, 'company_address', False),
        grant_fn=lambda grant: _zipcode_guard(grant, 'awardee_address', False)
    )),
    ('PrimeAwardeeBusinessTypes', DeriveValues(procurement='bus_types', award='business_types_desc')),
    ('PrimaryPlaceOfPerformanceCityName', copy_prime_field('principle_place_city')),
    ('PrimaryPlaceOfPerformanceStateCode', copy_prime_field('principle_place_state')),
    ('PrimaryPlaceOfPerformanceStateName', copy_prime_field('principle_place_state_name')),
    ('PrimaryPlaceOfPerformanceZIP+4', copy_prime_field('principle_place_zip')),
    ('PrimaryPlaceOfPerformanceCongressionalDistrict', copy_prime_field('principle_place_district')),
    ('PrimaryPlaceOfPerformanceCountryCode', copy_prime_field('principle_place_country')),
    ('PrimaryPlaceOfPerformanceCountryName', DeriveValues(
        procurement_fn=lambda contract: _country_name(contract.principle_place_country),
        grant_fn=lambda grant: _country_name(grant.principle_place_country)
    )),
    ('AwardDescription', DeriveValues(grant='project_description', award='award_description')),
    ('NAICS', DeriveValues(procurement='naics')),
    ('NAICS_Description', lambda models: models.naics_desc),
    ('CFDA_Numbers', DeriveValues(
        grant_fn=lambda grant: _extract_naics(grant.cfda_numbers, 'numbers')
    )),
    ('CFDA_Titles', DeriveValues(
        grant_fn=lambda grant: _extract_naics(grant.cfda_numbers, 'titles')
    )),

    # Sub-Award Properties
    ('SubAwardType', lambda subaward: _determine_sub_award_type(subaward)),
    ('SubAwardReportYear', copy_prime_field('report_period_year')),
    ('SubAwardReportMonth', copy_prime_field('report_period_mon')),
    ('SubawardNumber', DeriveValues('subcontract_num', 'subaward_num')),
    ('SubAwardAmount', DeriveValues('subcontract_amount', 'subaward_amount')),
    ('SubAwardActionDate', DeriveValues('subcontract_date', 'subaward_date')),
    ('SubAwardeeOrRecipientUniqueIdentifier', copy_subaward_field('duns')),
    ('SubAwardeeOrRecipientLegalEntityName', DeriveValues('company_name', 'awardee_name')),
    ('SubAwardeeDoingBusinessAsName', copy_subaward_field('dba_name')),
    ('SubAwardeeUltimateParentUniqueIdentifier', copy_subaward_field('parent_duns')),
    ('SubAwardeeUltimateParentLegalEntityName', DeriveValues(
        subcontract='parent_company_name',
        subgrant_fn=lambda subgrant: _derive_duns_name(subgrant.parent_duns)
    )),
    ('SubAwardeeLegalEntityCountryCode', DeriveValues('company_address_country', 'awardee_address_country')),
    ('SubAwardeeLegalEntityCountryName', DeriveValues(
        subcontract_fn=lambda subcontract: _country_name(subcontract.company_address_country),
        subgrant_fn=lambda subgrant: _country_name(subgrant.awardee_address_country)
    )),
    ('SubAwardeeLegalEntityAddressLine1', DeriveValues('company_address_street', 'awardee_address_street')),
    ('SubAwardeeLegalEntityCityName', DeriveValues('company_address_city', 'awardee_address_city')),
    ('SubAwardeeLegalEntityStateCode', DeriveValues('company_address_state', 'awardee_address_state')),
    ('SubAwardeeLegalEntityStateName', DeriveValues('company_address_state_name', 'awardee_address_state_name')),
    ('SubAwardeeLegalEntityZIP+4', DeriveValues(
        subcontract_fn=lambda subcontract: _zipcode_guard(subcontract, 'company_address', True),
        subgrant_fn=lambda subgrant: _zipcode_guard(subgrant, 'awardee_address', True)
    )),
    ('SubAwardeeLegalEntityCongressionalDistrict', DeriveValues('company_address_district',
                                                                'awardee_address_district')),
    ('SubAwardeeLegalEntityForeignPostalCode', DeriveValues(
        subcontract_fn=lambda subcontract: _zipcode_guard(subcontract, 'company_address', False),
        subgrant_fn=lambda subgrant: _zipcode_guard(subgrant, 'awardee_address', False)
    )),
    ('SubAwardeeBusinessTypes', DeriveValues(
        subcontract='bus_types',
        subgrant_fn=lambda subgrant: _derive_duns_business_types(subgrant.duns)
    )),
    ('SubAwardeePlaceOfPerformanceCityName', copy_subaward_field('principle_place_city')),
    ('SubAwardeePlaceOfPerformanceStateCode', copy_subaward_field('principle_place_state')),
    ('SubAwardeePlaceOfPerformanceStateName', copy_subaward_field('principle_place_state_name')),
    ('SubAwardeePlaceOfPerformanceZIP+4', copy_subaward_field('principle_place_zip')),
    ('SubAwardeePlaceOfPerformanceCongressionalDistrict', copy_subaward_field('principle_place_district')),
    ('SubAwardeePlaceOfPerformanceCountryCode', copy_subaward_field('principle_place_country')),
    ('SubAwardeePlaceOfPerformanceCountryName', DeriveValues(
        subcontract_fn=lambda subcontract: _country_name(subcontract.principle_place_country),
        subgrant_fn=lambda subgrant: _country_name(subgrant.principle_place_country)
    )),
    ('SubAwardDescription', DeriveValues('overall_description', 'project_description')),
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
])


def submission_procurements(submission_id):
    """ Fetch procurements and subcontracts

        Args:
            submission_id: submission to get data from

        Yields:
            ModelRows representing procurement data (award, procurement, subcontract)
    """
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
    """ Fetch grants and subgrants

        Args:
            submission_id: submission to get data from

        Yields:
            ModelRows representing grant data (award, grant, subgrant)
    """
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
    """ Generated OrderedDicts representing File F rows. Subawards are filtered to those relevant to a particular
        submissionId

        Args:
            submission_id: submission to get data from

        Yields:
            OrderedDict representing File F row
    """

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
