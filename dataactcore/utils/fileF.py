from collections import namedtuple, OrderedDict
import itertools

import iso3166

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.fsrs import FSRSGrant, FSRSProcurement, FSRSSubcontract, FSRSSubgrant
from dataactcore.models.stagingModels import AwardFinancialAssistance, AwardProcurement
from dataactcore.models.domainModels import DUNS
from dataactbroker.helpers.generic_helper import fy

from sqlalchemy.orm import outerjoin, aliased
from sqlalchemy import func, select

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
    if isinstance(model, FSRSProcurement):
        unique_key_attrs = ['CONT_AW']
        unique_fields = ['contract_agency_code', 'contract_idv_agency_code', 'contract_number', 'idv_reference_number']
    elif isinstance(model, FSRSGrant):
        unique_key_attrs = ['ASST_AW']
        unique_fields = ['federal_agency_id', 'fain']

    for unique_field in unique_fields:
        unique_key_attrs.append(getattr(model, unique_field) or '-none-')
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


def _extract_cfda(cfda_numbers, type):
    """ Get the cfda codes or titles

        Args:
            cfda_numbers: cfda_numbers string to parse
            type: whether to return 'numbers' or 'titles'

        Returns:
            cfda codes or titles, or None if invalid
    """
    if cfda_numbers:
        cfda_list = [cfda_entry.strip() for cfda_entry in cfda_numbers.split(';')]
        if type == 'numbers':
            return ','.join([cfda_entry.split(' ')[0] for cfda_entry in cfda_list if cfda_entry])
        elif type == 'titles':
            return ','.join([cfda_entry[cfda_entry.index(' ')+1:] for cfda_entry in cfda_list if cfda_entry])


class DeriveValues:
    """ Derive values (either by copying or applying logic from one of our existing models """
    # Order to check fields
    MODEL_TYPES = ('subcontract', 'subgrant', 'procurement', 'grant', 'award', 'grant_pduns', 'subgrant_pduns',
                   'subgrant_duns')

    def __init__(self, subcontract=None, subgrant=None, procurement=None, grant=None, award=None, procurement_fn=None,
                 grant_fn=None, subcontract_fn=None, subgrant_fn=None, award_fn=None, grant_pduns=None,
                 grant_pduns_fn=None, subgrant_pduns=None, subgrant_pduns_fn=None, subgrant_duns=None,
                 subgrant_duns_fn=None):
        # Copy fields
        self.procurement_field = procurement
        self.subcontract_field = subcontract
        self.grant_field = grant
        self.subgrant_field = subgrant
        self.award_field = award
        self.grant_pduns_field = grant_pduns
        self.subgrant_pduns_field = subgrant_pduns
        self.subgrant_duns_field = subgrant_duns

        # Logic functions
        self.procurement_fn = procurement_fn
        self.subcontract_fn = subcontract_fn
        self.grant_fn = grant_fn
        self.subgrant_fn = subgrant_fn
        self.award_fn = award_fn
        self.grant_pduns_fn = grant_pduns_fn
        self.subgrant_pduns_fn = subgrant_pduns_fn
        self.subgrant_duns_fn = subgrant_duns_fn

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
    ['award', 'procurement', 'subcontract', 'grant', 'subgrant', 'grant_pduns', 'subgrant_pduns', 'subgrant_duns'])
ModelRow.__new__.__defaults__ = (None, None, None, None, None, None, None, None)


# A collection of mappers (callables which convert a ModelRow into a string to
# be placed in a CSV cell), keyed by the CSV column name for that cell. Order
# matters as it defines the CSV column order
mappings = OrderedDict([
    # Prime Award Properties
    ('PrimeAwardUniqueKey', DeriveValues(
        procurement_fn=lambda contract: _prime_unique_id(contract),
        grant_fn=lambda grant: _prime_unique_id(grant)
    )),
    ('PrimeAwardID', DeriveValues(procurement='contract_number', grant='fain')),
    ('ParentAwardID', DeriveValues(procurement='idv_reference_number')),
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
    ('FundingSubTierAgencyName', DeriveValues(procurement='funding_agency_name', award='funding_sub_tier_agency_na')),
    ('FundingOfficeCode', DeriveValues(procurement='funding_office_id', award='funding_office_code')),
    ('FundingOfficeName', DeriveValues(procurement='funding_office_name', award='funding_office_name')),
    ('AwardeeOrRecipientUniqueIdentifier', copy_prime_field('duns')),
    ('AwardeeOrRecipientLegalEntityName', DeriveValues(procurement='company_name', grant='awardee_name')),
    ('Vendor Doing As Business Name', copy_prime_field('dba_name')),
    ('UltimateParentUniqueIdentifier', copy_prime_field('parent_duns')),
    ('UltimateParentLegalEntityName', DeriveValues(
        procurement='parent_company_name',
        grant_pduns='legal_business_name'
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
    ('NAICS_Description', DeriveValues(award='naics_description')),
    ('CFDA_Numbers', DeriveValues(
        grant_fn=lambda grant: _extract_cfda(grant.cfda_numbers, 'numbers')
    )),
    ('CFDA_Titles', DeriveValues(
        grant_fn=lambda grant: _extract_cfda(grant.cfda_numbers, 'titles')
    )),

    # Sub-Award Properties
    ('SubAwardType', DeriveValues(
        subcontract_fn=lambda subaward: _determine_sub_award_type(subaward),
        subgrant_fn=lambda subaward: _determine_sub_award_type(subaward)
    )),
    ('SubAwardReportYear', copy_prime_field('report_period_year')),
    ('SubAwardReportMonth', copy_prime_field('report_period_mon')),
    ('SubAwardNumber', DeriveValues('subcontract_num', 'subaward_num')),
    ('SubAwardAmount', DeriveValues('subcontract_amount', 'subaward_amount')),
    ('SubAwardActionDate', DeriveValues('subcontract_date', 'subaward_date')),
    ('SubAwardeeOrRecipientUniqueIdentifier', copy_subaward_field('duns')),
    ('SubAwardeeOrRecipientLegalEntityName', DeriveValues('company_name', 'awardee_name')),
    ('SubAwardeeDoingBusinessAsName', copy_subaward_field('dba_name')),
    ('SubAwardeeUltimateParentUniqueIdentifier', copy_subaward_field('parent_duns')),
    ('SubAwardeeUltimateParentLegalEntityName', DeriveValues(
        subcontract='parent_company_name',
        subgrant_pduns='legal_business_name'
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
    ('SubAwardeeBusinessTypes', DeriveValues(subcontract='bus_types', subgrant_duns='business_types_codes')),
    ('SubAwardPlaceOfPerformanceCityName', copy_subaward_field('principle_place_city')),
    ('SubAwardPlaceOfPerformanceStateCode', copy_subaward_field('principle_place_state')),
    ('SubAwardPlaceOfPerformanceStateName', copy_subaward_field('principle_place_state_name')),
    ('SubAwardPlaceOfPerformanceZIP+4', copy_subaward_field('principle_place_zip')),
    ('SubAwardPlaceOfPerformanceCongressionalDistrict', copy_subaward_field('principle_place_district')),
    ('SubAwardPlaceOfPerformanceCountryCode', copy_subaward_field('principle_place_country')),
    ('SubAwardPlaceOfPerformanceCountryName', DeriveValues(
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
                                AwardProcurement.submission_id, AwardProcurement.awarding_agency_code,
                                AwardProcurement.awarding_agency_name, AwardProcurement.funding_agency_code,
                                AwardProcurement.funding_agency_name).\
        filter(AwardProcurement.submission_id == submission_id).distinct().cte("award_proc_sub")

    results = sess.query(award_proc_sub, FSRSProcurement, FSRSSubcontract).\
        filter(FSRSProcurement.contract_number == award_proc_sub.c.piid).\
        filter(FSRSProcurement.idv_reference_number.isnot_distinct_from(award_proc_sub.c.parent_award_id)).\
        filter(FSRSProcurement.contracting_office_aid == award_proc_sub.c.awarding_sub_tier_agency_c).\
        filter(FSRSSubcontract.parent_id == FSRSProcurement.id)

    # The cte returns a set of columns, not an AwardProcurement object, so we have to unpack each column
    for award_piid, award_parent_id, award_naics_desc, award_sub_tier, award_sub_id, award_code, award_name, \
        fund_code, fund_name, proc, sub in results:
        # need to combine those columns again here so we can get a proper ModelRow
        award = AwardProcurement(piid=award_piid, parent_award_id=award_parent_id, naics_description=award_naics_desc,
                                 awarding_sub_tier_agency_c=award_sub_tier, submission_id=award_sub_id,
                                 awarding_agency_code=award_code, awarding_agency_name=award_name,
                                 funding_agency_code=fund_code, funding_agency_name=fund_name)
        yield ModelRow(award, proc, sub)

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

    afa_sub = sess.query(AwardFinancialAssistance.fain, AwardFinancialAssistance.submission_id,
                         AwardFinancialAssistance.awarding_agency_code, AwardFinancialAssistance.awarding_agency_name,
                         AwardFinancialAssistance.awarding_office_code, AwardFinancialAssistance.awarding_office_name,
                         AwardFinancialAssistance.funding_agency_code, AwardFinancialAssistance.funding_agency_name,
                         AwardFinancialAssistance.funding_office_code, AwardFinancialAssistance.funding_office_name,
                         AwardFinancialAssistance.business_types_desc, AwardFinancialAssistance.award_description,
                         AwardFinancialAssistance.awarding_sub_tier_agency_n,
                         AwardFinancialAssistance.funding_sub_tier_agency_co,
                         AwardFinancialAssistance.funding_sub_tier_agency_na).\
        filter(AwardFinancialAssistance.submission_id == submission_id).distinct().cte("afa_sub")

    grand_pduns_from = select([DUNS.awardee_or_recipient_uniqu, DUNS.legal_business_name,
                               func.row_number().over(partition_by=DUNS.awardee_or_recipient_uniqu).label('row')]). \
        select_from(outerjoin(FSRSGrant, DUNS, FSRSGrant.parent_duns == DUNS.awardee_or_recipient_uniqu)).\
        order_by(DUNS.activation_date.desc()).alias('grand_pduns_from')

    grant_pduns = sess.query(grand_pduns_from.c.awardee_or_recipient_uniqu, grand_pduns_from.c.legal_business_name). \
        filter(grand_pduns_from.c.row == 1).cte("grant_pduns")

    sub_pduns_from = select([DUNS.awardee_or_recipient_uniqu, DUNS.legal_business_name,
                            func.row_number().over(partition_by=DUNS.awardee_or_recipient_uniqu).label('row')]). \
        select_from(outerjoin(FSRSSubgrant, DUNS, FSRSSubgrant.parent_duns == DUNS.awardee_or_recipient_uniqu)). \
        order_by(DUNS.activation_date.desc()).alias('sub_pduns_from')

    subgrant_pduns = sess.query(sub_pduns_from.c.awardee_or_recipient_uniqu, sub_pduns_from.c.legal_business_name). \
        filter(sub_pduns_from.c.row == 1).cte("subgrant_pduns")

    sub_duns_from = select([DUNS.awardee_or_recipient_uniqu, DUNS.business_types_codes,
                            func.row_number().over(partition_by=DUNS.awardee_or_recipient_uniqu).label('row')]). \
        select_from(outerjoin(FSRSSubgrant, DUNS, FSRSSubgrant.duns == DUNS.awardee_or_recipient_uniqu)). \
        order_by(DUNS.activation_date.desc()).alias('sub_duns_from')

    subgrant_duns = sess.query(sub_duns_from.c.awardee_or_recipient_uniqu, sub_duns_from.c.business_types_codes). \
        filter(sub_duns_from.c.row == 1).cte("subgrant_duns")

    triplets = sess.query(afa_sub, FSRSGrant, FSRSSubgrant, grant_pduns.c.legal_business_name,
                          subgrant_pduns.c.legal_business_name, subgrant_duns.c.business_types_codes). \
        join(FSRSGrant, FSRSGrant.fain == afa_sub.c.fain). \
        join(FSRSSubgrant, FSRSSubgrant.parent_id == FSRSGrant.id). \
        outerjoin(grant_pduns, FSRSGrant.parent_duns == grant_pduns.c.awardee_or_recipient_uniqu). \
        outerjoin(subgrant_pduns, FSRSSubgrant.parent_duns == subgrant_pduns.c.awardee_or_recipient_uniqu). \
        outerjoin(subgrant_duns, FSRSSubgrant.duns == subgrant_duns.c.awardee_or_recipient_uniqu)

    # The cte returns a set of columns, not an AwardFinancialAssistance object, so we have to unpack each column
    for afa_sub_fain, afa_sub_id, award_code, award_name, award_office_code, award_office_name, fund_code, fund_name, \
        fund_office_code, fund_office_name, bus_types, award_desc, award_sub_name, fund_sub_code, fund_sub_name, grant, \
        sub, grant_pduns_name, sub_pduns_name, sub_duns_bus in triplets:
        # need to combine those columns again here so we can get a proper ModelRow
        award = AwardFinancialAssistance(fain=afa_sub_fain, submission_id=afa_sub_id, awarding_agency_code=award_code,
                                         awarding_agency_name=award_name, awarding_office_code=award_office_code,
                                         awarding_office_name=award_office_name, funding_agency_code=fund_code,
                                         funding_agency_name=fund_name, funding_office_code=fund_office_code,
                                         funding_office_name=fund_office_name, business_types_desc=bus_types,
                                         award_description=award_desc, awarding_sub_tier_agency_n=award_sub_name,
                                         funding_sub_tier_agency_co=fund_sub_code,
                                         funding_sub_tier_agency_na=fund_sub_name)
        award.naics_description = None
        grant_pduns = DUNS(legal_business_name=grant_pduns_name)
        subgrant_pduns = DUNS(legal_business_name=sub_pduns_name)
        subgrant_duns = DUNS(business_types_codes=sub_duns_bus)
        yield ModelRow(award, grant=grant, subgrant=sub, grant_pduns=grant_pduns, subgrant_pduns=subgrant_pduns,
                       subgrant_duns=subgrant_duns)

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
