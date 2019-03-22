from dataactcore.utils import fileF
from dataactbroker.helpers.generic_helper import fy
from dataactcore.models.fsrs import FSRSProcurement
from tests.unit.dataactcore.factories.fsrs import (FSRSGrantFactory, FSRSProcurementFactory, FSRSSubcontractFactory,
                                                   FSRSSubgrantFactory)
from tests.unit.dataactcore.factories.staging import AwardFinancialAssistanceFactory, AwardProcurementFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.domain import DunsFactory, CountryCodeFactory

EXPECTED_COLS = [
    'PrimeAwardUniqueKey',
    'PrimeAwardID',
    'ParentAwardID',
    'PrimeAwardAmount',
    'ActionDate',
    'PrimeAwardFiscalYear',
    'AwardingAgencyCode',
    'AwardingAgencyName',
    'AwardingSubTierAgencyCode',
    'AwardingSubTierAgencyName',
    'AwardingOfficeCode',
    'AwardingOfficeName',
    'FundingAgencyCode',
    'FundingAgencyName',
    'FundingSubTierAgencyCode',
    'FundingSubTierAgencyName',
    'FundingOfficeCode',
    'FundingOfficeName',
    'AwardeeOrRecipientUniqueIdentifier',
    'AwardeeOrRecipientLegalEntityName',
    'Vendor Doing As Business Name',
    'UltimateParentUniqueIdentifier',
    'UltimateParentLegalEntityName',
    'LegalEntityCountryCode',
    'LegalEntityCountryName',
    'LegalEntityAddressLine1',
    'LegalEntityCityName',
    'LegalEntityStateCode',
    'LegalEntityStateName',
    'LegalEntityZIP+4',
    'LegalEntityCongressionalDistrict',
    'LegalEntityForeignPostalCode',
    'PrimeAwardeeBusinessTypes',
    'PrimaryPlaceOfPerformanceCityName',
    'PrimaryPlaceOfPerformanceStateCode',
    'PrimaryPlaceOfPerformanceStateName',
    'PrimaryPlaceOfPerformanceZIP+4',
    'PrimaryPlaceOfPerformanceCongressionalDistrict',
    'PrimaryPlaceOfPerformanceCountryCode',
    'PrimaryPlaceOfPerformanceCountryName',
    'AwardDescription',
    'NAICS',
    'NAICS_Description',
    'CFDA_Numbers',
    'CFDA_Titles',
    'SubAwardType',
    'SubAwardReportYear',
    'SubAwardReportMonth',
    'SubAwardNumber',
    'SubAwardAmount',
    'SubAwardActionDate',
    'SubAwardeeOrRecipientUniqueIdentifier',
    'SubAwardeeOrRecipientLegalEntityName',
    'SubAwardeeDoingBusinessAsName',
    'SubAwardeeUltimateParentUniqueIdentifier',
    'SubAwardeeUltimateParentLegalEntityName',
    'SubAwardeeLegalEntityCountryCode',
    'SubAwardeeLegalEntityCountryName',
    'SubAwardeeLegalEntityAddressLine1',
    'SubAwardeeLegalEntityCityName',
    'SubAwardeeLegalEntityStateCode',
    'SubAwardeeLegalEntityStateName',
    'SubAwardeeLegalEntityZIP+4',
    'SubAwardeeLegalEntityCongressionalDistrict',
    'SubAwardeeLegalEntityForeignPostalCode',
    'SubAwardeeBusinessTypes',
    'SubAwardPlaceOfPerformanceCityName',
    'SubAwardPlaceOfPerformanceStateCode',
    'SubAwardPlaceOfPerformanceStateName',
    'SubAwardPlaceOfPerformanceZIP+4',
    'SubAwardPlaceOfPerformanceCongressionalDistrict',
    'SubAwardPlaceOfPerformanceCountryCode',
    'SubAwardPlaceOfPerformanceCountryName',
    'SubAwardDescription',
    'SubAwardeeHighCompOfficer1FullName',
    'SubAwardeeHighCompOfficer1Amount',
    'SubAwardeeHighCompOfficer2FullName',
    'SubAwardeeHighCompOfficer2Amount',
    'SubAwardeeHighCompOfficer3FullName',
    'SubAwardeeHighCompOfficer3Amount',
    'SubAwardeeHighCompOfficer4FullName',
    'SubAwardeeHighCompOfficer4Amount',
    'SubAwardeeHighCompOfficer5FullName',
    'SubAwardeeHighCompOfficer5Amount'
]


def generate_unique_key(fsrs_award, d_file):
    """ Helper function representing the cfda psql functions """
    if isinstance(fsrs_award, FSRSProcurement):
        unique_fields = ['CONT']
        if d_file.idv_type is not None:
            unique_fields.extend(['IDV', fsrs_award.contract_number, fsrs_award.contract_agency_code])
        else:
            unique_fields.extend(['AWD', fsrs_award.contract_number, fsrs_award.contract_agency_code,
                                  fsrs_award.contract_idv_agency_code, fsrs_award.idv_reference_number])
    else:
        unique_fields = ['ASST']
        if d_file.record_type == '1':
            unique_fields.extend(['AGG', d_file.uri, fsrs_award.federal_agency_id])
        else:
            unique_fields.extend(['NON', fsrs_award.fain, fsrs_award.federal_agency_id])
    return '_'.join([unique_field or '-NONE-' for unique_field in unique_fields]).upper()


def extract_cfda(field, type):
    """ Helper function representing the cfda psql functions """
    extracted_values = []
    if field:
        entries = [entry.strip() for entry in field.split(';')]
        if type == 'numbers':
            extracted_values = [entry[:entry.index(' ')] for entry in entries]
        else:
            extracted_values = [entry[entry.index(' ')+1:] for entry in entries]
    return ', '.join(extracted_values)


def reference_data(sess):
    parent_duns = DunsFactory(awardee_or_recipient_uniqu='987654321', legal_business_name='TEST PARENT DUNS')
    duns = DunsFactory(awardee_or_recipient_uniqu='123456789', legal_business_name='TEST DUNS',
                       business_types_codes=['A', 'B', 'C'])
    dom_country = CountryCodeFactory(country_code='USA', country_name='UNITED STATES')
    int_country = CountryCodeFactory(country_code='INT', country_name='INTERNATIONAL')
    sess.add_all([parent_duns, duns, dom_country, int_country])
    return parent_duns, duns, dom_country, int_country


def replicate_contract_results(sub, d1, contract, sub_contract, parent_duns, duns, dom_country, int_country):
    """ Helper function for contract results """
    return (
        generate_unique_key(contract, d1),
        contract.contract_number,
        contract.idv_reference_number,
        contract.dollar_obligated,
        contract.date_signed,
        'FY{}'.format(fy(contract.date_signed)),
        d1.awarding_agency_code,
        d1.awarding_agency_name,
        contract.contracting_office_aid,
        contract.contracting_office_aname,
        contract.contracting_office_id,
        contract.contracting_office_name,
        d1.funding_agency_code,
        d1.funding_agency_name,
        contract.funding_agency_id,
        contract.funding_agency_name,
        contract.funding_office_id,
        contract.funding_office_name,
        contract.duns,
        contract.company_name,
        contract.dba_name,
        contract.parent_duns,
        contract.parent_company_name,
        contract.company_address_country,
        dom_country.country_name,
        contract.company_address_street,
        contract.company_address_city,
        contract.company_address_state,
        contract.company_address_state_name,
        contract.company_address_zip,
        contract.company_address_district,
        None,
        contract.bus_types,
        contract.principle_place_city,
        contract.principle_place_state,
        contract.principle_place_state_name,
        contract.principle_place_zip,
        contract.principle_place_district,
        contract.principle_place_country,
        int_country.country_name,
        d1.award_description,
        contract.naics,
        d1.naics_description,
        None,
        None,
        'sub-contract',
        contract.report_period_year,
        contract.report_period_mon,
        sub_contract.subcontract_num,
        sub_contract.subcontract_amount,
        sub_contract.subcontract_date,
        sub_contract.duns,
        sub_contract.company_name,
        sub_contract.dba_name,
        sub_contract.parent_duns,
        sub_contract.parent_company_name,
        sub_contract.company_address_country,
        int_country.country_name,
        sub_contract.company_address_street,
        sub_contract.company_address_city,
        sub_contract.company_address_state,
        sub_contract.company_address_state_name,
        None,
        sub_contract.company_address_district,
        sub_contract.company_address_zip,
        sub_contract.bus_types,
        sub_contract.principle_place_city,
        sub_contract.principle_place_state,
        sub_contract.principle_place_state_name,
        sub_contract.principle_place_zip,
        sub_contract.principle_place_district,
        sub_contract.principle_place_country,
        dom_country.country_name,
        sub_contract.overall_description,
        sub_contract.top_paid_fullname_1,
        sub_contract.top_paid_amount_1,
        sub_contract.top_paid_fullname_2,
        sub_contract.top_paid_amount_2,
        sub_contract.top_paid_fullname_3,
        sub_contract.top_paid_amount_3,
        sub_contract.top_paid_fullname_4,
        sub_contract.top_paid_amount_4,
        sub_contract.top_paid_fullname_5,
        sub_contract.top_paid_amount_5
    )


def test_generate_f_file_queries_contracts(database, monkeypatch):
    """ generate_f_file_queries should provide queries representing halves of F file data related to a submission
        This will cover contracts records.
    """
    sess = database.session

    parent_duns, duns, dom_country, int_country = reference_data(sess)

    # Setup - create awards, procurements, subcontracts
    sub = SubmissionFactory(submission_id=1)
    d1_awd = AwardProcurementFactory(
        submission_id=sub.submission_id,
        idv_type=None
    )
    contract_awd = FSRSProcurementFactory(
        contract_number=d1_awd.piid,
        idv_reference_number=d1_awd.parent_award_id,
        contracting_office_aid=d1_awd.awarding_sub_tier_agency_c,
        company_address_country=dom_country.country_code,
        principle_place_country=int_country.country_code,
        duns=duns.awardee_or_recipient_uniqu
    )
    sub_contract_awd = FSRSSubcontractFactory(
        parent=contract_awd,
        company_address_country=int_country.country_code,
        principle_place_country=dom_country.country_code
    )
    d1_idv = AwardProcurementFactory(
        submission_id=sub.submission_id,
        idv_type='C'
    )
    contract_idv = FSRSProcurementFactory(
        contract_number=d1_idv.piid,
        idv_reference_number=d1_idv.parent_award_id,
        contracting_office_aid=d1_idv.awarding_sub_tier_agency_c,
        company_address_country=dom_country.country_code,
        principle_place_country=int_country.country_code,
        duns=duns.awardee_or_recipient_uniqu
    )
    sub_contract_idv = FSRSSubcontractFactory(
        parent=contract_idv,
        company_address_country=int_country.country_code,
        principle_place_country=dom_country.country_code
    )

    sess.add_all([sub, d1_awd, contract_awd, sub_contract_awd, d1_idv, contract_idv, sub_contract_idv])
    sess.commit()

    # Gather the sql
    contract_query, _ = fileF.generate_f_file_queries(sub.submission_id)

    # Get the records
    contracts_records = sess.execute(contract_query)
    contracts_cols = contracts_records.keys()
    contracts_results = contracts_records.fetchall()

    # Expected Results
    expected_contracts = [
        replicate_contract_results(sub, d1_awd, contract_awd, sub_contract_awd, parent_duns, duns, dom_country,
                                   int_country),
        replicate_contract_results(sub, d1_idv, contract_idv, sub_contract_idv, parent_duns, duns, dom_country,
                                   int_country)
    ]

    assert sorted(contracts_results, key=lambda result: result[0]) == expected_contracts
    assert contracts_cols == EXPECTED_COLS


def replicate_grant_results(sub, d2, grant, sub_grant, parent_duns, duns, dom_country, int_country):
    """ Helper function for grant results """
    return (
        generate_unique_key(grant, d2),
        grant.fain,
        None,
        grant.total_fed_funding_amount,
        grant.obligation_date,
        'FY{}'.format(fy(grant.obligation_date)),
        d2.awarding_agency_code,
        d2.awarding_agency_name,
        grant.federal_agency_id,
        d2.awarding_sub_tier_agency_n,
        d2.awarding_office_code,
        d2.awarding_office_name,
        d2.funding_agency_code,
        d2.funding_agency_name,
        d2.funding_sub_tier_agency_co,
        d2.funding_sub_tier_agency_na,
        d2.funding_office_code,
        d2.funding_office_name,
        grant.duns,
        grant.awardee_name,
        grant.dba_name,
        grant.parent_duns,
        parent_duns.legal_business_name,
        grant.awardee_address_country,
        int_country.country_name,
        grant.awardee_address_street,
        grant.awardee_address_city,
        grant.awardee_address_state,
        grant.awardee_address_state_name,
        None,
        grant.awardee_address_district,
        grant.awardee_address_zip,
        d2.business_types_desc,
        grant.principle_place_city,
        grant.principle_place_state,
        grant.principle_place_state_name,
        grant.principle_place_zip,
        grant.principle_place_district,
        grant.principle_place_country,
        dom_country.country_name,
        grant.project_description,
        None,
        None,
        extract_cfda(grant.cfda_numbers, 'numbers'),
        extract_cfda(grant.cfda_numbers, 'titles'),
        'sub-grant',
        grant.report_period_year,
        grant.report_period_mon,
        sub_grant.subaward_num,
        sub_grant.subaward_amount,
        sub_grant.subaward_date,
        sub_grant.duns,
        sub_grant.awardee_name,
        sub_grant.dba_name,
        sub_grant.parent_duns,
        parent_duns.legal_business_name,
        sub_grant.awardee_address_country,
        dom_country.country_name,
        sub_grant.awardee_address_street,
        sub_grant.awardee_address_city,
        sub_grant.awardee_address_state,
        sub_grant.awardee_address_state_name,
        sub_grant.awardee_address_zip,
        sub_grant.awardee_address_district,
        None,
        ', '.join(parent_duns.business_types_codes),
        sub_grant.principle_place_city,
        sub_grant.principle_place_state,
        sub_grant.principle_place_state_name,
        sub_grant.principle_place_zip,
        sub_grant.principle_place_district,
        sub_grant.principle_place_country,
        int_country.country_name,
        sub_grant.project_description,
        sub_grant.top_paid_fullname_1,
        sub_grant.top_paid_amount_1,
        sub_grant.top_paid_fullname_2,
        sub_grant.top_paid_amount_2,
        sub_grant.top_paid_fullname_3,
        sub_grant.top_paid_amount_3,
        sub_grant.top_paid_fullname_4,
        sub_grant.top_paid_amount_4,
        sub_grant.top_paid_fullname_5,
        sub_grant.top_paid_amount_5
    )


def test_generate_f_file_queries_grants(database, monkeypatch):
    """ generate_f_file_queries should provide queries representing halves of F file data related to a submission
        This will cover grants records.
    """
    # Setup - create awards, procurements/grants, subawards
    sess = database.session

    parent_duns, duns, dom_country, int_country = reference_data(sess)

    # Setup - create awards, procurements, subcontracts
    sub = SubmissionFactory(submission_id=1)
    d2_non = AwardFinancialAssistanceFactory(
        submission_id=sub.submission_id,
        record_type='2'
    )
    grant_non = FSRSGrantFactory(
        fain=d2_non.fain,
        awardee_address_country=int_country.country_code,
        principle_place_country=dom_country.country_code,
        parent_duns=parent_duns.awardee_or_recipient_uniqu,
        cfda_numbers='00.001 CFDA 1; 00.002 CFDA 2'
    )
    sub_grant_non = FSRSSubgrantFactory(
        parent=grant_non,
        awardee_address_country=dom_country.country_code,
        principle_place_country=int_country.country_code,
        parent_duns=parent_duns.awardee_or_recipient_uniqu,
        duns=duns.awardee_or_recipient_uniqu
    )
    d2_agg = AwardFinancialAssistanceFactory(
        submission_id=sub.submission_id,
        record_type='1'
    )
    grant_agg = FSRSGrantFactory(
        fain=d2_agg.fain,
        awardee_address_country=int_country.country_code,
        principle_place_country=dom_country.country_code,
        parent_duns=parent_duns.awardee_or_recipient_uniqu,
        cfda_numbers='00.003 CFDA 3'
    )
    sub_grant_agg = FSRSSubgrantFactory(
        parent=grant_agg,
        awardee_address_country=dom_country.country_code,
        principle_place_country=int_country.country_code,
        parent_duns=parent_duns.awardee_or_recipient_uniqu,
        duns=duns.awardee_or_recipient_uniqu
    )
    sess.add_all([sub, d2_non, grant_non, sub_grant_non, d2_agg, grant_agg, sub_grant_agg])
    sess.commit()

    # Gather the sql
    _, grant_query = fileF.generate_f_file_queries(sub.submission_id)

    # Get the records
    grants_records = sess.execute(grant_query)
    grants_cols = grants_records.keys()
    grants_results = grants_records.fetchall()

    # Expected Results
    expected_grants_results = [
        replicate_grant_results(sub, d2_agg, grant_agg, sub_grant_agg, parent_duns, duns, dom_country, int_country),
        replicate_grant_results(sub, d2_non, grant_non, sub_grant_non, parent_duns, duns, dom_country, int_country),
    ]

    assert sorted(grants_results, key=lambda result: result[0]) == expected_grants_results
    assert grants_cols == EXPECTED_COLS
