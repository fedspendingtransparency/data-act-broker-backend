from datetime import datetime

from dataactcore.scripts.populate_subaward_table import populate_subaward_table, fix_broken_links
from dataactbroker.helpers.generic_helper import fy
from dataactcore.models.fsrs import Subaward
from tests.unit.dataactcore.factories.fsrs import (FSRSGrantFactory, FSRSProcurementFactory, FSRSSubcontractFactory,
                                                   FSRSSubgrantFactory)
from tests.unit.dataactcore.factories.staging import PublishedAwardFinancialAssistanceFactory, \
    DetachedAwardProcurementFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.domain import DunsFactory, CountryCodeFactory


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


def compare_contract_results(sub, d1, contract, sub_contract, parent_duns, duns, dom_country, int_country, created_at,
                             updated_at, debug=False):
    """ Helper function for contract results """
    attr = {
        'created_at': created_at,
        'updated_at': updated_at,
        'id': sub.id,

        'unique_award_key': d1.unique_award_key,
        'award_amount': contract.dollar_obligated,
        'action_date': str(contract.date_signed),
        'fy': 'FY{}'.format(fy(contract.date_signed)),
        'awarding_agency_code': d1.awarding_agency_code,
        'awarding_agency_name': d1.awarding_agency_name,
        'awarding_sub_tier_agency_c': contract.contracting_office_aid,
        'awarding_sub_tier_agency_n': contract.contracting_office_aname,
        'awarding_office_code': contract.contracting_office_id,
        'awarding_office_name': contract.contracting_office_name,
        'funding_agency_code': d1.funding_agency_code,
        'funding_agency_name': d1.funding_agency_name,
        'funding_sub_tier_agency_co': contract.funding_agency_id,
        'funding_sub_tier_agency_na': contract.funding_agency_name,
        'funding_office_code': contract.funding_office_id,
        'funding_office_name': contract.funding_office_name,
        'awardee_or_recipient_uniqu': contract.duns,
        'awardee_or_recipient_legal': contract.company_name,
        'dba_name': contract.dba_name,
        'ultimate_parent_unique_ide': contract.parent_duns,
        'ultimate_parent_legal_enti': contract.parent_company_name,
        'legal_entity_country_code': contract.company_address_country,
        'legal_entity_country_name': dom_country.country_name,
        'legal_entity_address_line1': contract.company_address_street,
        'legal_entity_city_name': contract.company_address_city,
        'legal_entity_state_code': contract.company_address_state,
        'legal_entity_state_name': contract.company_address_state_name,
        'legal_entity_zip': contract.company_address_zip,
        'legal_entity_congressional': contract.company_address_district,
        'legal_entity_foreign_posta': None,
        'business_types': contract.bus_types,
        'place_of_perform_city_name': contract.principle_place_city,
        'place_of_perform_state_code': contract.principle_place_state,
        'place_of_perform_state_name': contract.principle_place_state_name,
        'place_of_performance_zip': contract.principle_place_zip,
        'place_of_perform_congressio': contract.principle_place_district,
        'place_of_perform_country_co': contract.principle_place_country,
        'place_of_perform_country_na': int_country.country_name,
        'award_description': d1.award_description,
        'naics': contract.naics,
        'naics_description': d1.naics_description,
        'cfda_numbers': None,
        'cfda_titles': None,

        'subaward_type': 'sub-contract',
        'subaward_report_year': contract.report_period_year,
        'subaward_report_month': contract.report_period_mon,
        'subaward_number': sub_contract.subcontract_num,
        'subaward_amount': sub_contract.subcontract_amount,
        'sub_action_date': str(sub_contract.subcontract_date),
        'sub_awardee_or_recipient_uniqu': sub_contract.duns,
        'sub_awardee_or_recipient_legal': sub_contract.company_name,
        'sub_dba_name': sub_contract.dba_name,
        'sub_ultimate_parent_unique_ide': sub_contract.parent_duns,
        'sub_ultimate_parent_legal_enti': sub_contract.parent_company_name,
        'sub_legal_entity_country_code': sub_contract.company_address_country,
        'sub_legal_entity_country_name': int_country.country_name,
        'sub_legal_entity_address_line1': sub_contract.company_address_street,
        'sub_legal_entity_city_name': sub_contract.company_address_city,
        'sub_legal_entity_state_code': sub_contract.company_address_state,
        'sub_legal_entity_state_name': sub_contract.company_address_state_name,
        'sub_legal_entity_zip': None,
        'sub_legal_entity_congressional': sub_contract.company_address_district,
        'sub_legal_entity_foreign_posta': sub_contract.company_address_zip,
        'sub_business_types': sub_contract.bus_types,
        'sub_place_of_perform_city_name': sub_contract.principle_place_city,
        'sub_place_of_perform_state_code': sub_contract.principle_place_state,
        'sub_place_of_perform_state_name': sub_contract.principle_place_state_name,
        'sub_place_of_performance_zip': sub_contract.principle_place_zip,
        'sub_place_of_perform_congressio': sub_contract.principle_place_district,
        'sub_place_of_perform_country_co': sub_contract.principle_place_country,
        'sub_place_of_perform_country_na': dom_country.country_name,
        'subaward_description': sub_contract.overall_description,
        'sub_high_comp_officer1_full_na': sub_contract.top_paid_fullname_1,
        'sub_high_comp_officer1_amount': sub_contract.top_paid_amount_1,
        'sub_high_comp_officer2_full_na': sub_contract.top_paid_fullname_2,
        'sub_high_comp_officer2_amount': sub_contract.top_paid_amount_2,
        'sub_high_comp_officer3_full_na': sub_contract.top_paid_fullname_3,
        'sub_high_comp_officer3_amount': sub_contract.top_paid_amount_3,
        'sub_high_comp_officer4_full_na': sub_contract.top_paid_fullname_4,
        'sub_high_comp_officer4_amount': sub_contract.top_paid_amount_4,
        'sub_high_comp_officer5_full_na': sub_contract.top_paid_fullname_5,
        'sub_high_comp_officer5_amount': sub_contract.top_paid_amount_5,

        'prime_id': contract.id,
        'internal_id': contract.internal_id,
        'date_submitted': contract.date_submitted.strftime('%Y-%m-%d %H:%M:%S.%f'),
        'report_type': contract.report_type,
        'transaction_type': contract.transaction_type,
        'program_title': contract.program_title,
        'contract_agency_code': contract.contract_agency_code,
        'contract_idv_agency_code': contract.contract_idv_agency_code,
        'grant_funding_agency_id': None,
        'grant_funding_agency_name': None,
        'federal_agency_name': None,
        'treasury_symbol': contract.treasury_symbol,
        'dunsplus4': None,
        'recovery_model_q1': str(contract.recovery_model_q1).lower(),
        'recovery_model_q2': str(contract.recovery_model_q2).lower(),
        'compensation_q1': None,
        'compensation_q2': None,
        'high_comp_officer1_full_na': contract.top_paid_fullname_1,
        'high_comp_officer1_amount': contract.top_paid_amount_1,
        'high_comp_officer2_full_na': contract.top_paid_fullname_2,
        'high_comp_officer2_amount': contract.top_paid_amount_2,
        'high_comp_officer3_full_na': contract.top_paid_fullname_3,
        'high_comp_officer3_amount': contract.top_paid_amount_3,
        'high_comp_officer4_full_na': contract.top_paid_fullname_4,
        'high_comp_officer4_amount': contract.top_paid_amount_4,
        'high_comp_officer5_full_na': contract.top_paid_fullname_5,
        'high_comp_officer5_amount': contract.top_paid_amount_5,
        'place_of_perform_street': contract.principle_place_street,
        'sub_id': sub_contract.id,
        'sub_parent_id': sub_contract.parent_id,
        'sub_federal_agency_id': None,
        'sub_federal_agency_name': None,
        'sub_funding_agency_id': sub_contract.funding_agency_id,
        'sub_funding_agency_name': sub_contract.funding_agency_name,
        'sub_funding_office_id': sub_contract.funding_office_id,
        'sub_funding_office_name': sub_contract.funding_office_name,
        'sub_naics': sub_contract.naics,
        'sub_cfda_numbers': None,
        'sub_dunsplus4': None,
        'sub_recovery_subcontract_amt': sub_contract.recovery_subcontract_amt,
        'sub_recovery_model_q1': str(sub_contract.recovery_model_q1).lower(),
        'sub_recovery_model_q2': str(sub_contract.recovery_model_q2).lower(),
        'sub_compensation_q1': None,
        'sub_compensation_q2': None,
        'sub_place_of_perform_street': sub_contract.principle_place_street
    }
    normal_compare = (attr.items() <= sub.__dict__.items())

    dash_attrs = {
        'award_id': contract.contract_number,
        'parent_award_id': contract.idv_reference_number,
    }
    dash_compare = True
    for da_name, da_value in dash_attrs.items():
        dash_compare &= (da_value.replace('-', '') == sub.__dict__[da_name].replace('-', ''))

    compare = normal_compare & dash_compare
    if debug and not compare:
        print(compare, normal_compare, dash_compare)
        print(sorted(attr.items()))
        print(sorted(sub.__dict__.items()))
    return compare


def test_generate_f_file_queries_contracts(database, monkeypatch):
    """ generate_f_file_queries should provide queries representing halves of F file data related to a submission
        This will cover contracts records.
    """
    sess = database.session
    sess.query(Subaward).delete(synchronize_session=False)
    sess.commit()

    parent_duns, duns, dom_country, int_country = reference_data(sess)

    # Setup - create awards, procurements, subcontract
    sub = SubmissionFactory(submission_id=1)
    d1_awd = DetachedAwardProcurementFactory(
        submission_id=sub.submission_id,
        idv_type=None,
        unique_award_key='AWD',
        piid='AWD-PIID-WITH-DASHES',
        parent_award_id='AWD-PARENT-AWARD-ID-WITH-DASHES'
    )
    contract_awd = FSRSProcurementFactory(
        contract_number=d1_awd.piid.replace('-', ''),
        idv_reference_number=d1_awd.parent_award_id.replace('-', ''),
        contracting_office_aid=d1_awd.awarding_sub_tier_agency_c,
        company_address_country=dom_country.country_code,
        principle_place_country=int_country.country_code,
        duns=duns.awardee_or_recipient_uniqu,
        date_signed=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_contract_awd = FSRSSubcontractFactory(
        parent=contract_awd,
        company_address_country=int_country.country_code,
        principle_place_country=dom_country.country_code,
        subcontract_date=datetime.now()
    )
    d1_idv = DetachedAwardProcurementFactory(
        submission_id=sub.submission_id,
        idv_type='C',
        unique_award_key='IDV',
        piid='IDV-PIID-WITH-DASHES',
        parent_award_id='IDV-PARENT-AWARD-ID-WITH-DASHES'
    )
    contract_idv = FSRSProcurementFactory(
        contract_number=d1_idv.piid.replace('-', ''),
        idv_reference_number=d1_idv.parent_award_id.replace('-', ''),
        contracting_office_aid=d1_idv.awarding_sub_tier_agency_c,
        company_address_country=dom_country.country_code,
        principle_place_country=int_country.country_code,
        duns=duns.awardee_or_recipient_uniqu,
        date_signed=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_contract_idv = FSRSSubcontractFactory(
        parent=contract_idv,
        company_address_country=int_country.country_code,
        principle_place_country=dom_country.country_code,
        subcontract_date=datetime.now()
    )

    sess.add_all([sub, d1_awd, contract_awd, sub_contract_awd, d1_idv, contract_idv, sub_contract_idv])
    sess.commit()

    # Gather the sql
    populate_subaward_table(sess, 'procurement_service', ids=[contract_awd.id, contract_idv.id])

    # Get the records
    contracts_results = sess.query(Subaward).order_by(Subaward.unique_award_key).all()

    created_at = updated_at = contracts_results[0].created_at

    # Expected Results
    assert compare_contract_results(contracts_results[0], d1_awd, contract_awd, sub_contract_awd, parent_duns, duns,
                                    dom_country, int_country, created_at, updated_at) is True
    assert compare_contract_results(contracts_results[1], d1_idv, contract_idv, sub_contract_idv, parent_duns, duns,
                                    dom_country, int_country, created_at, updated_at) is True


def compare_grant_results(sub, d2, grant, sub_grant, parent_duns, duns, dom_country, int_country, created_at,
                          updated_at, debug=False):
    """ Helper function for grant results """
    attr = {
        'created_at': created_at,
        'updated_at': updated_at,
        'id': sub.id,

        'unique_award_key': d2.unique_award_key,
        'parent_award_id': None,
        'award_amount': grant.total_fed_funding_amount,
        'action_date': str(grant.obligation_date),
        'fy': 'FY{}'.format(fy(grant.obligation_date)),
        'awarding_agency_code': d2.awarding_agency_code,
        'awarding_agency_name': d2.awarding_agency_name,
        'awarding_sub_tier_agency_c': grant.federal_agency_id,
        'awarding_sub_tier_agency_n': d2.awarding_sub_tier_agency_n,
        'awarding_office_code': d2.awarding_office_code,
        'awarding_office_name': d2.awarding_office_name,
        'funding_agency_code': d2.funding_agency_code,
        'funding_agency_name': d2.funding_agency_name,
        'funding_sub_tier_agency_co': d2.funding_sub_tier_agency_co,
        'funding_sub_tier_agency_na': d2.funding_sub_tier_agency_na,
        'funding_office_code': d2.funding_office_code,
        'funding_office_name': d2.funding_office_name,
        'awardee_or_recipient_uniqu': grant.duns,
        'awardee_or_recipient_legal': grant.awardee_name,
        'dba_name': grant.dba_name,
        'ultimate_parent_unique_ide': grant.parent_duns,
        'ultimate_parent_legal_enti': parent_duns.legal_business_name,
        'legal_entity_country_code': grant.awardee_address_country,
        'legal_entity_country_name': int_country.country_name,
        'legal_entity_address_line1': grant.awardee_address_street,
        'legal_entity_city_name': grant.awardee_address_city,
        'legal_entity_state_code': grant.awardee_address_state,
        'legal_entity_state_name': grant.awardee_address_state_name,
        'legal_entity_zip': None,
        'legal_entity_congressional': grant.awardee_address_district,
        'legal_entity_foreign_posta': grant.awardee_address_zip,
        'business_types': d2.business_types_desc,
        'place_of_perform_city_name': grant.principle_place_city,
        'place_of_perform_state_code': grant.principle_place_state,
        'place_of_perform_state_name': grant.principle_place_state_name,
        'place_of_performance_zip': grant.principle_place_zip,
        'place_of_perform_congressio': grant.principle_place_district,
        'place_of_perform_country_co': grant.principle_place_country,
        'place_of_perform_country_na': dom_country.country_name,
        'award_description': grant.project_description,
        'naics': None,
        'naics_description': None,
        'cfda_numbers': extract_cfda(grant.cfda_numbers, 'numbers'),
        'cfda_titles': extract_cfda(grant.cfda_numbers, 'titles'),

        'subaward_type': 'sub-grant',
        'subaward_report_year': grant.report_period_year,
        'subaward_report_month': grant.report_period_mon,
        'subaward_number': sub_grant.subaward_num,
        'subaward_amount': sub_grant.subaward_amount,
        'sub_action_date': str(sub_grant.subaward_date),
        'sub_awardee_or_recipient_uniqu': sub_grant.duns,
        'sub_awardee_or_recipient_legal': sub_grant.awardee_name,
        'sub_dba_name': sub_grant.dba_name,
        'sub_ultimate_parent_unique_ide': sub_grant.parent_duns,
        'sub_ultimate_parent_legal_enti': parent_duns.legal_business_name,
        'sub_legal_entity_country_code': sub_grant.awardee_address_country,
        'sub_legal_entity_country_name': dom_country.country_name,
        'sub_legal_entity_address_line1': sub_grant.awardee_address_street,
        'sub_legal_entity_city_name': sub_grant.awardee_address_city,
        'sub_legal_entity_state_code': sub_grant.awardee_address_state,
        'sub_legal_entity_state_name': sub_grant.awardee_address_state_name,
        'sub_legal_entity_zip': sub_grant.awardee_address_zip,
        'sub_legal_entity_congressional': sub_grant.awardee_address_district,
        'sub_legal_entity_foreign_posta': None,
        'sub_business_types': ', '.join(parent_duns.business_types_codes),
        'sub_place_of_perform_city_name': sub_grant.principle_place_city,
        'sub_place_of_perform_state_code': sub_grant.principle_place_state,
        'sub_place_of_perform_state_name': sub_grant.principle_place_state_name,
        'sub_place_of_performance_zip': sub_grant.principle_place_zip,
        'sub_place_of_perform_congressio': sub_grant.principle_place_district,
        'sub_place_of_perform_country_co': sub_grant.principle_place_country,
        'sub_place_of_perform_country_na': int_country.country_name,
        'subaward_description': sub_grant.project_description,
        'sub_high_comp_officer1_full_na': sub_grant.top_paid_fullname_1,
        'sub_high_comp_officer1_amount': sub_grant.top_paid_amount_1,
        'sub_high_comp_officer2_full_na': sub_grant.top_paid_fullname_2,
        'sub_high_comp_officer2_amount': sub_grant.top_paid_amount_2,
        'sub_high_comp_officer3_full_na': sub_grant.top_paid_fullname_3,
        'sub_high_comp_officer3_amount': sub_grant.top_paid_amount_3,
        'sub_high_comp_officer4_full_na': sub_grant.top_paid_fullname_4,
        'sub_high_comp_officer4_amount': sub_grant.top_paid_amount_4,
        'sub_high_comp_officer5_full_na': sub_grant.top_paid_fullname_5,
        'sub_high_comp_officer5_amount': sub_grant.top_paid_amount_5,

        'prime_id': grant.id,
        'internal_id': grant.internal_id,
        'date_submitted': grant.date_submitted.strftime('%Y-%m-%d %H:%M:%S.%f'),
        'report_type': None,
        'transaction_type': None,
        'program_title': None,
        'contract_agency_code': None,
        'contract_idv_agency_code': None,
        'grant_funding_agency_id': grant.funding_agency_id,
        'grant_funding_agency_name': grant.funding_agency_name,
        'federal_agency_name': grant.federal_agency_name,
        'treasury_symbol': None,
        'dunsplus4': grant.dunsplus4,
        'recovery_model_q1': None,
        'recovery_model_q2': None,
        'compensation_q1': str(grant.compensation_q1).lower(),
        'compensation_q2': str(grant.compensation_q2).lower(),
        'high_comp_officer1_full_na': grant.top_paid_fullname_1,
        'high_comp_officer1_amount': grant.top_paid_amount_1,
        'high_comp_officer2_full_na': grant.top_paid_fullname_2,
        'high_comp_officer2_amount': grant.top_paid_amount_2,
        'high_comp_officer3_full_na': grant.top_paid_fullname_3,
        'high_comp_officer3_amount': grant.top_paid_amount_3,
        'high_comp_officer4_full_na': grant.top_paid_fullname_4,
        'high_comp_officer4_amount': grant.top_paid_amount_4,
        'high_comp_officer5_full_na': grant.top_paid_fullname_5,
        'high_comp_officer5_amount': grant.top_paid_amount_5,
        'place_of_perform_street': grant.principle_place_street,
        'sub_id': sub_grant.id,
        'sub_parent_id': sub_grant.parent_id,
        'sub_federal_agency_id': sub_grant.federal_agency_id,
        'sub_federal_agency_name': sub_grant.federal_agency_name,
        'sub_funding_agency_id': sub_grant.funding_agency_id,
        'sub_funding_agency_name': sub_grant.funding_agency_name,
        'sub_funding_office_id': None,
        'sub_funding_office_name': None,
        'sub_naics': None,
        'sub_cfda_numbers': sub_grant.cfda_numbers,
        'sub_dunsplus4': sub_grant.dunsplus4,
        'sub_recovery_subcontract_amt': None,
        'sub_recovery_model_q1': None,
        'sub_recovery_model_q2': None,
        'sub_compensation_q1': str(sub_grant.compensation_q1).lower(),
        'sub_compensation_q2': str(sub_grant.compensation_q2).lower(),
        'sub_place_of_perform_street': sub_grant.principle_place_street
    }
    normal_compare = (attr.items() <= sub.__dict__.items())

    dash_attrs = {
        'award_id': grant.fain,
    }
    dash_compare = True
    for da_name, da_value in dash_attrs.items():
        dash_compare &= (da_value.replace('-', '') == sub.__dict__[da_name].replace('-', ''))

    compare = normal_compare & dash_compare
    if debug and not compare:
        print(sorted(attr.items()))
        print(sorted(sub.__dict__.items()))
    return compare


def test_generate_f_file_queries_grants(database, monkeypatch):
    """ generate_f_file_queries should provide queries representing halves of F file data related to a submission
        This will cover grants records.
    """
    # Setup - create awards, procurements/grants, subawards
    sess = database.session
    sess.query(Subaward).delete(synchronize_session=False)
    sess.commit()

    parent_duns, duns, dom_country, int_country = reference_data(sess)

    # Setup - create awards, procurements, subcontracts
    sub = SubmissionFactory(submission_id=1)
    d2_non = PublishedAwardFinancialAssistanceFactory(
        submission_id=sub.submission_id,
        record_type='2',
        unique_award_key='NON',
        fain='NON-FAIN-WITH-DASHES',
        is_active=True
    )
    grant_non = FSRSGrantFactory(
        fain=d2_non.fain.replace('-', ''),
        awardee_address_country=int_country.country_code,
        principle_place_country=dom_country.country_code,
        parent_duns=parent_duns.awardee_or_recipient_uniqu,
        cfda_numbers='00.001 CFDA 1; 00.002 CFDA 2',
        obligation_date=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_grant_non = FSRSSubgrantFactory(
        parent=grant_non,
        awardee_address_country=dom_country.country_code,
        principle_place_country=int_country.country_code,
        parent_duns=parent_duns.awardee_or_recipient_uniqu,
        duns=duns.awardee_or_recipient_uniqu,
        subaward_date=datetime.now()
    )
    d2_agg = PublishedAwardFinancialAssistanceFactory(
        submission_id=sub.submission_id,
        record_type='1',
        unique_award_key='AGG',
        fain='AGG-FAIN-WITH-DASHES',
        is_active=True
    )
    grant_agg = FSRSGrantFactory(
        fain=d2_agg.fain.replace('-', ''),
        awardee_address_country=int_country.country_code,
        principle_place_country=dom_country.country_code,
        parent_duns=parent_duns.awardee_or_recipient_uniqu,
        cfda_numbers='00.003 CFDA 3',
        obligation_date=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_grant_agg = FSRSSubgrantFactory(
        parent=grant_agg,
        awardee_address_country=dom_country.country_code,
        principle_place_country=int_country.country_code,
        parent_duns=parent_duns.awardee_or_recipient_uniqu,
        duns=duns.awardee_or_recipient_uniqu,
        subaward_date=datetime.now()
    )
    sess.add_all([sub, d2_non, grant_non, sub_grant_non, d2_agg, grant_agg, sub_grant_agg])
    sess.commit()

    # Gather the sql
    populate_subaward_table(sess, 'grant_service', ids=[grant_agg.id, grant_non.id])

    # Get the records
    grants_results = sess.query(Subaward).order_by(Subaward.unique_award_key).all()

    created_at = updated_at = grants_results[0].created_at

    # Expected Results
    assert compare_grant_results(grants_results[0], d2_agg, grant_agg, sub_grant_agg, parent_duns, duns, dom_country,
                                 int_country, created_at, updated_at) is True
    assert compare_grant_results(grants_results[1], d2_non, grant_non, sub_grant_non, parent_duns, duns, dom_country,
                                 int_country, created_at, updated_at) is True


def test_fix_broken_links(database, monkeypatch):
    """ Ensure that fix_broken_links works as intended """

    # Setup - create awards, procurements/grants, subawards
    sess = database.session
    sess.query(Subaward).delete(synchronize_session=False)
    sess.commit()

    parent_duns, duns, dom_country, int_country = reference_data(sess)
    min_date = '2019-06-06'
    award_updated_at = '2019-06-07'

    # Setup - Grants
    sub = SubmissionFactory(submission_id=1)
    d2_non = PublishedAwardFinancialAssistanceFactory(
        submission_id=sub.submission_id,
        record_type='2',
        unique_award_key='NON',
        fain='NON-FAIN-WITH-DASHES',
        is_active=True,
        updated_at=award_updated_at
    )
    grant_non = FSRSGrantFactory(
        fain=d2_non.fain.replace('-', ''),
        awardee_address_country=int_country.country_code,
        principle_place_country=dom_country.country_code,
        parent_duns=parent_duns.awardee_or_recipient_uniqu,
        cfda_numbers='00.001 CFDA 1; 00.002 CFDA 2',
        obligation_date=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_grant_non = FSRSSubgrantFactory(
        parent=grant_non,
        awardee_address_country=dom_country.country_code,
        principle_place_country=int_country.country_code,
        parent_duns=parent_duns.awardee_or_recipient_uniqu,
        duns=duns.awardee_or_recipient_uniqu,
        subaward_date=datetime.now()
    )
    d2_agg = PublishedAwardFinancialAssistanceFactory(
        submission_id=sub.submission_id,
        record_type='1',
        unique_award_key='AGG',
        fain='AGG-FAIN-WITH-DASHES',
        is_active=True,
        updated_at=award_updated_at
    )
    grant_agg = FSRSGrantFactory(
        fain=d2_agg.fain.replace('-', ''),
        awardee_address_country=int_country.country_code,
        principle_place_country=dom_country.country_code,
        parent_duns=parent_duns.awardee_or_recipient_uniqu,
        cfda_numbers='00.003 CFDA 3',
        obligation_date=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_grant_agg = FSRSSubgrantFactory(
        parent=grant_agg,
        awardee_address_country=dom_country.country_code,
        principle_place_country=int_country.country_code,
        parent_duns=parent_duns.awardee_or_recipient_uniqu,
        duns=duns.awardee_or_recipient_uniqu,
        subaward_date=datetime.now()
    )

    # Setup - Contracts
    d1_awd = DetachedAwardProcurementFactory(
        submission_id=sub.submission_id,
        idv_type=None,
        unique_award_key='AWD',
        piid='AWD-PIID-WITH-DASHES',
        parent_award_id='AWD-PARENT-AWARD-ID-WITH-DASHES',
        updated_at=award_updated_at
    )
    contract_awd = FSRSProcurementFactory(
        contract_number=d1_awd.piid.replace('-', ''),
        idv_reference_number=d1_awd.parent_award_id.replace('-', ''),
        contracting_office_aid=d1_awd.awarding_sub_tier_agency_c,
        company_address_country=dom_country.country_code,
        principle_place_country=int_country.country_code,
        duns=duns.awardee_or_recipient_uniqu,
        date_signed=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_contract_awd = FSRSSubcontractFactory(
        parent=contract_awd,
        company_address_country=int_country.country_code,
        principle_place_country=dom_country.country_code,
        subcontract_date=datetime.now()
    )
    d1_idv = DetachedAwardProcurementFactory(
        submission_id=sub.submission_id,
        idv_type='C',
        unique_award_key='IDV',
        piid='IDV-PIID-WITH-DASHES',
        parent_award_id='IDV-PARENT-AWARD-IDV-WITH-DASHES',
        updated_at=award_updated_at
    )
    contract_idv = FSRSProcurementFactory(
        contract_number=d1_idv.piid.replace('-', ''),
        idv_reference_number=d1_idv.parent_award_id.replace('-', ''),
        contracting_office_aid=d1_idv.awarding_sub_tier_agency_c,
        company_address_country=dom_country.country_code,
        principle_place_country=int_country.country_code,
        duns=duns.awardee_or_recipient_uniqu,
        date_signed=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_contract_idv = FSRSSubcontractFactory(
        parent=contract_idv,
        company_address_country=int_country.country_code,
        principle_place_country=dom_country.country_code,
        subcontract_date=datetime.now()
    )

    # Note: not including d1/d2 data
    sess.add_all([sub, contract_awd, sub_contract_awd, contract_idv, sub_contract_idv])
    sess.add_all([sub, grant_non, sub_grant_non, grant_agg, sub_grant_agg])
    sess.commit()

    populate_subaward_table(sess, 'procurement_service', ids=[contract_awd.id, contract_idv.id])
    populate_subaward_table(sess, 'grant_service', ids=[grant_agg.id, grant_non.id])

    contracts_results = sess.query(Subaward).order_by(Subaward.unique_award_key).\
        filter(Subaward.subaward_type == 'sub-contract').all()
    grants_results = sess.query(Subaward).order_by(Subaward.unique_award_key).\
        filter(Subaward.subaward_type == 'sub-grant').all()
    original_ids = [result.id for result in contracts_results + grants_results]

    grant_created_at = grant_updated_at = grants_results[0].created_at
    contract_created_at = contract_updated_at = contracts_results[0].created_at

    # Expected Results - should be False as the award isn't provided
    assert compare_contract_results(contracts_results[0], d1_awd, contract_awd, sub_contract_awd, parent_duns, duns,
                                    dom_country, int_country, contract_created_at, contract_updated_at) is False
    assert compare_contract_results(contracts_results[1], d1_idv, contract_idv, sub_contract_idv, parent_duns, duns,
                                    dom_country, int_country, contract_created_at, contract_updated_at) is False
    assert compare_grant_results(grants_results[0], d2_agg, grant_agg, sub_grant_agg, parent_duns, duns, dom_country,
                                 int_country, grant_created_at, grant_updated_at) is False
    assert compare_grant_results(grants_results[1], d2_non, grant_non, sub_grant_non, parent_duns, duns, dom_country,
                                 int_country, grant_created_at, grant_updated_at) is False

    # now add the awards and fix the broken links
    sess.add_all([d1_awd, d2_non, d1_idv, d2_agg])
    sess.commit()

    updated_proc_count = fix_broken_links(sess, 'procurement_service', min_date=min_date)
    updated_grant_count = fix_broken_links(sess, 'grant_service', min_date=min_date)

    assert updated_proc_count == updated_grant_count == 2

    contracts_results = sess.query(Subaward).order_by(Subaward.unique_award_key).\
        filter(Subaward.subaward_type == 'sub-contract').all()
    grants_results = sess.query(Subaward).order_by(Subaward.unique_award_key).\
        filter(Subaward.subaward_type == 'sub-grant').all()
    updated_ids = [result.id for result in contracts_results + grants_results]

    contract_created_at = contracts_results[0].created_at
    contract_updated_at = contracts_results[0].updated_at
    grant_created_at = grants_results[0].created_at
    grant_updated_at = grants_results[0].updated_at

    # Expected Results - should now be True as the award is now available
    assert compare_contract_results(contracts_results[0], d1_awd, contract_awd, sub_contract_awd, parent_duns, duns,
                                    dom_country, int_country, contract_created_at, contract_updated_at) is True
    assert compare_contract_results(contracts_results[1], d1_idv, contract_idv, sub_contract_idv, parent_duns, duns,
                                    dom_country, int_country, contract_created_at, contract_updated_at) is True
    assert compare_grant_results(grants_results[0], d2_agg, grant_agg, sub_grant_agg, parent_duns, duns, dom_country,
                                 int_country, grant_created_at, grant_updated_at) is True
    assert compare_grant_results(grants_results[1], d2_non, grant_non, sub_grant_non, parent_duns, duns, dom_country,
                                 int_country, grant_created_at, grant_updated_at) is True

    # Ensuring only updates occurred, no deletes/inserts
    assert set(original_ids) == set(updated_ids)
