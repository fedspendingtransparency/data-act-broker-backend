from datetime import datetime

from dataactcore.scripts.populate_subaward_table import populate_subaward_table, fix_broken_links
from dataactbroker.helpers.generic_helper import fy
from dataactcore.models.fsrs import Subaward
from tests.unit.dataactcore.factories.fsrs import (FSRSGrantFactory, FSRSProcurementFactory, FSRSSubcontractFactory,
                                                   FSRSSubgrantFactory)
from tests.unit.dataactcore.factories.staging import PublishedFABSFactory, DetachedAwardProcurementFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.domain import (SAMRecipientFactory, CFDAProgramFactory, CountryCodeFactory,
                                                     CountyCodeFactory, ZipsGroupedFactory)


def extract_cfda(field, type):
    """ Helper function representing the cfda psql functions """
    extracted_values = []
    if field:
        entries = [entry.strip() for entry in field.split(';')]
        if type == 'numbers':
            extracted_values = [entry[:entry.index(' ')] for entry in entries]
        else:
            extracted_values = [entry[entry.index(' ') + 1:] for entry in entries]
    return ', '.join(extracted_values)


def reference_data(sess):
    parent_recipient = SAMRecipientFactory(uei='Cba987654321', legal_business_name='TEST PARENT RECIPIENT')
    recipient = SAMRecipientFactory(uei='123456789aBc', legal_business_name='TEST RECIPIENT',
                                    business_types=['TYPE 1', 'TYPE 2', 'TYPE 3'])
    dom_country = CountryCodeFactory(country_code='USA', country_code_2_char='US', country_name='UNITED STATES')
    dom_zip = ZipsGroupedFactory(zip5='12345', county_number='543', state_abbreviation='VA')
    dom_county = CountyCodeFactory(county_number='543', county_name='DOM COUNTY', state_code='VA')
    int_country = CountryCodeFactory(country_code='INT', country_code_2_char='IT', country_name='INTERNATIONAL')
    cfda_1 = CFDAProgramFactory(program_number='10.000', program_title='TEST NUMBER 1')
    cfda_2 = CFDAProgramFactory(program_number='20.000', program_title='TEST NUMBER 2')
    sess.add_all([parent_recipient, recipient, dom_country, dom_zip, dom_county, int_country,
                  cfda_1, cfda_2])
    return (parent_recipient, recipient, dom_country, dom_zip, dom_county, int_country, cfda_1,
            cfda_2)


def compare_contract_results(sub, d1, contract, sub_contract, dom_country, dom_zip, dom_county, int_country,
                             created_at, updated_at, debug=True):
    """ Helper function for contract results """
    attr = {
        'created_at': created_at,
        'updated_at': updated_at,
        'id': sub.id,

        'unique_award_key': d1.unique_award_key,
        'award_amount': d1.total_obligated_amount,
        'action_date': str(d1.action_date),
        'fy': 'FY{}'.format(fy(d1.action_date)),
        'awarding_agency_code': d1.awarding_agency_code,
        'awarding_agency_name': d1.awarding_agency_name,
        'awarding_sub_tier_agency_c': d1.awarding_sub_tier_agency_c,
        'awarding_sub_tier_agency_n': d1.awarding_sub_tier_agency_n,
        'awarding_office_code': d1.awarding_office_code,
        'awarding_office_name': d1.awarding_office_name,
        'funding_agency_code': d1.funding_agency_code,
        'funding_agency_name': d1.funding_agency_name,
        'funding_sub_tier_agency_co': d1.funding_sub_tier_agency_co,
        'funding_sub_tier_agency_na': d1.funding_sub_tier_agency_na,
        'funding_office_code': d1.funding_office_code,
        'funding_office_name': d1.funding_office_name,
        'awardee_or_recipient_uniqu': contract.duns,
        'awardee_or_recipient_uei': d1.awardee_or_recipient_uei,
        'awardee_or_recipient_legal': d1.awardee_or_recipient_legal,
        'dba_name': d1.vendor_doing_as_business_n,
        'ultimate_parent_unique_ide': d1.ultimate_parent_unique_ide,
        'ultimate_parent_uei': d1.ultimate_parent_uei,
        'ultimate_parent_legal_enti': d1.ultimate_parent_legal_enti,
        'legal_entity_country_code': d1.legal_entity_country_code,
        'legal_entity_country_name': d1.legal_entity_country_name,
        'legal_entity_address_line1': d1.legal_entity_address_line1,
        'legal_entity_city_name': d1.legal_entity_city_name,
        'legal_entity_state_code': d1.legal_entity_state_code,
        'legal_entity_state_name': d1.legal_entity_state_descrip,
        'legal_entity_zip': d1.legal_entity_zip4,
        "legal_entity_county_code": d1.legal_entity_county_code,
        "legal_entity_county_name": d1.legal_entity_county_name,
        'legal_entity_congressional': d1.legal_entity_congressional,
        'legal_entity_foreign_posta': None,
        'business_types': contract.bus_types,
        'place_of_perform_city_name': d1.place_of_perform_city_name,
        'place_of_perform_state_code': d1.place_of_performance_state,
        'place_of_perform_state_name': d1.place_of_perfor_state_desc,
        'place_of_performance_zip': d1.place_of_performance_zip4a,
        'place_of_performance_county_code': d1.place_of_perform_county_co,
        'place_of_performance_county_name': d1.place_of_perform_county_na,
        'place_of_perform_congressio': d1.place_of_performance_congr,
        'place_of_perform_country_co': d1.place_of_perform_country_c,
        'place_of_perform_country_na': d1.place_of_perf_country_desc,
        'award_description': d1.award_description,
        'naics': d1.naics,
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
        'sub_awardee_or_recipient_uei': sub_contract.uei_number,
        'sub_awardee_or_recipient_legal': sub_contract.company_name,
        'sub_dba_name': sub_contract.dba_name,
        'sub_ultimate_parent_unique_ide': sub_contract.parent_duns,
        'sub_ultimate_parent_legal_enti': sub_contract.parent_company_name,
        'sub_legal_entity_country_code': int_country.country_code,
        'sub_legal_entity_country_name': int_country.country_name,
        'sub_legal_entity_address_line1': sub_contract.company_address_street,
        'sub_legal_entity_city_name': sub_contract.company_address_city,
        'sub_legal_entity_state_code': sub_contract.company_address_state,
        'sub_legal_entity_state_name': sub_contract.company_address_state_name,
        'sub_legal_entity_zip': None,
        'sub_legal_entity_county_code': None,
        'sub_legal_entity_county_name': None,
        'sub_legal_entity_congressional': sub_contract.company_address_district,
        'sub_legal_entity_foreign_posta': sub_contract.company_address_zip,
        'sub_business_types': sub_contract.bus_types,
        'sub_place_of_perform_city_name': sub_contract.principle_place_city,
        'sub_place_of_perform_state_code': sub_contract.principle_place_state,
        'sub_place_of_perform_state_name': sub_contract.principle_place_state_name,
        'sub_place_of_performance_zip': sub_contract.principle_place_zip,
        'sub_place_of_performance_county_code': dom_zip.county_number,
        'sub_place_of_performance_county_name': dom_county.county_name,
        'sub_place_of_perform_congressio': sub_contract.principle_place_district,
        'sub_place_of_perform_country_co': dom_country.country_code,
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
        'high_comp_officer1_full_na': d1.high_comp_officer1_full_na,
        'high_comp_officer1_amount': d1.high_comp_officer1_amount,
        'high_comp_officer2_full_na': d1.high_comp_officer2_full_na,
        'high_comp_officer2_amount': d1.high_comp_officer2_amount,
        'high_comp_officer3_full_na': d1.high_comp_officer3_full_na,
        'high_comp_officer3_amount': d1.high_comp_officer3_amount,
        'high_comp_officer4_full_na': d1.high_comp_officer4_full_na,
        'high_comp_officer4_amount': d1.high_comp_officer4_amount,
        'high_comp_officer5_full_na': d1.high_comp_officer5_full_na,
        'high_comp_officer5_amount': d1.high_comp_officer5_amount,
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
        if sub.__dict__[da_name] is not None:
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

    _, _, dom_country, dom_zip, dom_county, int_country, _, _ = reference_data(sess)

    # Setup - create awards, procurements, subcontract
    sub = SubmissionFactory(submission_id=1)
    d1_awd = DetachedAwardProcurementFactory(
        submission_id=sub.submission_id,
        idv_type=None,
        unique_award_key='AWD',
        piid='AWD-PIID-WITH-DASHES',
        parent_award_id='AWD-PARENT-AWARD-ID-WITH-DASHES',
        legal_entity_country_code=dom_country.country_code
    )
    d1_awd_2 = DetachedAwardProcurementFactory(
        submission_id=sub.submission_id,
        idv_type=None,
        unique_award_key='AWD',
        piid='AWD-PIID-WITH-DASHES',
        parent_award_id='AWD-PARENT-AWARD-ID-WITH-DASHES',
        legal_entity_country_code=dom_country.country_code
    )
    contract_awd = FSRSProcurementFactory(
        contract_number=d1_awd.piid.replace('-', ''),
        idv_reference_number=d1_awd.parent_award_id.replace('-', ''),
        contracting_office_aid=d1_awd.awarding_sub_tier_agency_c,
        company_address_country=dom_country.country_code,
        principle_place_country=int_country.country_code,
        date_signed=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_contract_awd = FSRSSubcontractFactory(
        parent=contract_awd,
        company_address_country=int_country.country_code,
        principle_place_country=dom_country.country_code,
        principle_place_zip=dom_zip.zip5 + '1234',
        subcontract_date=datetime.now()
    )
    d1_idv = DetachedAwardProcurementFactory(
        submission_id=sub.submission_id,
        idv_type='C',
        unique_award_key='IDV',
        piid='IDV-PIID-WITH-DASHES',
        parent_award_id='IDV-PARENT-AWARD-ID-WITH-DASHES',
        action_date='2020-01-01',
        legal_entity_country_code=dom_country.country_code
    )
    d1_idv_2 = DetachedAwardProcurementFactory(
        submission_id=sub.submission_id,
        idv_type='C',
        unique_award_key='IDV',
        piid='IDV-PIID-WITH-DASHES',
        parent_award_id='IDV-PARENT-AWARD-ID-WITH-DASHES',
        action_date='2020-01-02',
        legal_entity_country_code=dom_country.country_code
    )
    contract_idv = FSRSProcurementFactory(
        contract_number=d1_idv.piid.replace('-', ''),
        idv_reference_number=d1_idv.parent_award_id.replace('-', ''),
        contracting_office_aid=d1_idv.awarding_sub_tier_agency_c,
        company_address_country=dom_country.country_code,
        principle_place_country=int_country.country_code,
        date_signed=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_contract_idv = FSRSSubcontractFactory(
        parent=contract_idv,
        company_address_country=int_country.country_code,
        principle_place_country=dom_country.country_code,
        principle_place_zip=dom_zip.zip5 + '1234',
        subcontract_date=datetime.now()
    )

    sess.add_all([sub, d1_awd, d1_awd_2, contract_awd, sub_contract_awd, d1_idv, d1_idv_2, contract_idv,
                  sub_contract_idv])
    sess.commit()

    # Gather the sql
    populate_subaward_table(sess, 'procurement_service', ids=[contract_awd.id, contract_idv.id])

    # Get the records
    contracts_results = sess.query(Subaward).order_by(Subaward.unique_award_key).all()
    assert len(contracts_results) == 2

    created_at = updated_at = contracts_results[0].created_at

    # Expected Results
    assert compare_contract_results(contracts_results[0], d1_awd, contract_awd, sub_contract_awd, dom_country, dom_zip,
                                    dom_county, int_country, created_at, updated_at) is True
    assert compare_contract_results(contracts_results[1], d1_idv, contract_idv, sub_contract_idv, dom_country, dom_zip,
                                    dom_county, int_country, created_at, updated_at) is True


def compare_grant_results(sub, fabs_base, fabs_latest, fabs_grouped, grant, sub_grant, parent_recipient, recipient,
                          dom_country, dom_zip, dom_county, int_country, created_at, updated_at, debug=True):
    """ Helper function for grant results """
    attr = {
        'created_at': created_at,
        'updated_at': updated_at,
        'id': sub.id,

        'unique_award_key': fabs_latest.unique_award_key,
        'parent_award_id': None,
        'award_amount': str(fabs_grouped['award_amount']),
        'action_date': str(fabs_base.action_date),
        'fy': 'FY{}'.format(fy(fabs_base.action_date)),
        'awarding_agency_code': fabs_latest.awarding_agency_code,
        'awarding_agency_name': fabs_latest.awarding_agency_name,
        'awarding_sub_tier_agency_c': fabs_latest.awarding_sub_tier_agency_c,
        'awarding_sub_tier_agency_n': fabs_latest.awarding_sub_tier_agency_n,
        'awarding_office_code': fabs_latest.awarding_office_code,
        'awarding_office_name': fabs_latest.awarding_office_name,
        'funding_agency_code': fabs_latest.funding_agency_code,
        'funding_agency_name': fabs_latest.funding_agency_name,
        'funding_sub_tier_agency_co': fabs_latest.funding_sub_tier_agency_co,
        'funding_sub_tier_agency_na': fabs_latest.funding_sub_tier_agency_na,
        'funding_office_code': fabs_latest.funding_office_code,
        'funding_office_name': fabs_latest.funding_office_name,
        'awardee_or_recipient_uniqu': fabs_latest.awardee_or_recipient_uniqu,
        'awardee_or_recipient_uei': fabs_latest.uei,
        'awardee_or_recipient_legal': fabs_latest.awardee_or_recipient_legal,
        'dba_name': recipient.dba_name,
        'ultimate_parent_unique_ide': fabs_latest.ultimate_parent_unique_ide,
        'ultimate_parent_uei': fabs_latest.ultimate_parent_uei,
        'ultimate_parent_legal_enti': fabs_latest.ultimate_parent_legal_enti,
        'legal_entity_country_code': int_country.country_code,
        'legal_entity_country_name': int_country.country_name,
        'legal_entity_address_line1': fabs_latest.legal_entity_address_line1,
        'legal_entity_city_name': fabs_latest.legal_entity_city_name,
        'legal_entity_state_code': fabs_latest.legal_entity_state_code,
        'legal_entity_state_name': fabs_latest.legal_entity_state_name,
        'legal_entity_zip': None,
        'legal_entity_county_code': fabs_latest.legal_entity_county_code,
        'legal_entity_county_name': fabs_latest.legal_entity_county_name,
        'legal_entity_congressional': fabs_latest.legal_entity_congressional,
        'legal_entity_foreign_posta': fabs_latest.legal_entity_foreign_posta,
        'business_types': fabs_latest.business_types_desc,
        'place_of_perform_city_name': fabs_latest.place_of_performance_city,
        'place_of_perform_state_code': fabs_latest.place_of_perfor_state_code,
        'place_of_perform_state_name': fabs_latest.place_of_perform_state_nam,
        'place_of_performance_zip': fabs_latest.place_of_performance_zip4a.replace('-', ''),
        'place_of_performance_county_code': fabs_latest.place_of_perform_county_co,
        'place_of_performance_county_name': fabs_latest.place_of_perform_county_na,
        'place_of_perform_congressio': fabs_latest.place_of_performance_congr,
        'place_of_perform_country_co': dom_country.country_code,
        'place_of_perform_country_na': dom_country.country_name,
        'award_description': fabs_base.award_description,
        'naics': None,
        'naics_description': None,

        'subaward_type': 'sub-grant',
        'subaward_report_year': grant.report_period_year,
        'subaward_report_month': grant.report_period_mon,
        'subaward_number': sub_grant.subaward_num,
        'subaward_amount': sub_grant.subaward_amount,
        'sub_action_date': str(sub_grant.subaward_date),
        'sub_awardee_or_recipient_uniqu': sub_grant.duns,
        'sub_awardee_or_recipient_uei': sub_grant.uei_number,
        'sub_awardee_or_recipient_legal': sub_grant.awardee_name,
        'sub_dba_name': sub_grant.dba_name,
        'sub_ultimate_parent_unique_ide': sub_grant.parent_duns,
        'sub_ultimate_parent_legal_enti': parent_recipient.legal_business_name,
        'sub_legal_entity_country_code': dom_country.country_code,
        'sub_legal_entity_country_name': dom_country.country_name,
        'sub_legal_entity_address_line1': sub_grant.awardee_address_street,
        'sub_legal_entity_city_name': sub_grant.awardee_address_city,
        'sub_legal_entity_state_code': sub_grant.awardee_address_state,
        'sub_legal_entity_state_name': sub_grant.awardee_address_state_name,
        'sub_legal_entity_zip': sub_grant.awardee_address_zip,
        'sub_legal_entity_county_code': dom_zip.county_number,
        'sub_legal_entity_county_name': dom_county.county_name,
        'sub_legal_entity_congressional': sub_grant.awardee_address_district,
        'sub_legal_entity_foreign_posta': None,
        'sub_business_types': ','.join(recipient.business_types) if recipient.business_types else None,
        'sub_place_of_perform_city_name': sub_grant.principle_place_city,
        'sub_place_of_perform_state_code': sub_grant.principle_place_state,
        'sub_place_of_perform_state_name': sub_grant.principle_place_state_name,
        'sub_place_of_performance_zip': sub_grant.principle_place_zip,
        'sub_place_of_performance_county_code': None,
        'sub_place_of_performance_county_name': None,
        'sub_place_of_perform_congressio': sub_grant.principle_place_district,
        'sub_place_of_perform_country_co': int_country.country_code,
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
        'high_comp_officer1_full_na': fabs_latest.high_comp_officer1_full_na,
        'high_comp_officer1_amount': fabs_latest.high_comp_officer1_amount,
        'high_comp_officer2_full_na': fabs_latest.high_comp_officer2_full_na,
        'high_comp_officer2_amount': fabs_latest.high_comp_officer2_amount,
        'high_comp_officer3_full_na': fabs_latest.high_comp_officer3_full_na,
        'high_comp_officer3_amount': fabs_latest.high_comp_officer3_amount,
        'high_comp_officer4_full_na': fabs_latest.high_comp_officer4_full_na,
        'high_comp_officer4_amount': fabs_latest.high_comp_officer4_amount,
        'high_comp_officer5_full_na': fabs_latest.high_comp_officer5_full_na,
        'high_comp_officer5_amount': fabs_latest.high_comp_officer5_amount,
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
        if sub.__dict__[da_name] is not None:
            dash_compare &= (da_value.replace('-', '') == sub.__dict__[da_name].replace('-', ''))

    array_attrs = {
        'cfda_numbers': fabs_grouped['cfda_num'],
        'cfda_titles': fabs_grouped['cfda_title']
    }
    array_compare = True
    for ar_name, ar_value in array_attrs.items():
        if sub.__dict__[ar_name] is not None:
            array_compare &= (sorted(ar_value) == sorted(sub.__dict__[ar_name]))

    compare = normal_compare & dash_compare & array_compare
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

    parent_recipient, recipient, dom_country, dom_zip, dom_county, int_country, cfda_1, cfda_2 = reference_data(sess)

    # Setup - create awards, procurements, subcontracts
    sub = SubmissionFactory(submission_id=1)
    # FABS Non-aggregate award with federal_agency_id/awarding_sub_tier_agency_c populated
    fabs_non_pop_subtier = PublishedFABSFactory(
        submission_id=sub.submission_id,
        record_type=2,
        unique_award_key='NON-POP-SUB',
        fain='NON-FAIN-WITH-DASHES-POP-SUB',
        awarding_sub_tier_agency_c='1234',
        uei=recipient.uei,
        is_active=True,
        action_date='2020-01-01',
        cfda_number=cfda_1.program_number
    )
    fabs_non_pop_subtier_2 = PublishedFABSFactory(
        submission_id=sub.submission_id,
        record_type=2,
        unique_award_key='NON-POP-SUB',
        fain='NON-FAIN-WITH-DASHES-POP-SUB',
        awarding_sub_tier_agency_c='1234',
        uei=recipient.uei,
        is_active=True,
        action_date='2020-01-02',
        cfda_number=cfda_2.program_number
    )
    grant_non_pop_subtier = FSRSGrantFactory(
        id=3,
        fain=fabs_non_pop_subtier.fain.replace('-', ''),
        federal_agency_id='1234',
        awardee_address_country=int_country.country_code_2_char,
        principle_place_country=dom_country.country_code,
        uei_number=recipient.uei,
        parent_uei=parent_recipient.uei,
        cfda_numbers='00.001 CFDA 1; 00.002 CFDA 2',
        obligation_date=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_grant_non_pop_subtier = FSRSSubgrantFactory(
        parent=grant_non_pop_subtier,
        awardee_address_country=dom_country.country_code,
        awardee_address_zip=dom_zip.zip5,
        principle_place_country=int_country.country_code_2_char,
        parent_uei=parent_recipient.uei,
        uei_number=recipient.uei,
        subaward_date=datetime.now()
    )
    # FABS Non-aggregate award with federal_agency_id NULL
    fabs_non_null_sub = PublishedFABSFactory(
        submission_id=sub.submission_id,
        record_type=2,
        unique_award_key='NON-NULL-SUB',
        fain='NON-FAIN-WITH-DASHES-NULL-SUB',
        awarding_sub_tier_agency_c='5678',
        uei=recipient.uei,
        is_active=True,
        action_date='2020-01-01',
        cfda_number=cfda_1.program_number
    )
    fabs_non_null_sub_2 = PublishedFABSFactory(
        submission_id=sub.submission_id,
        record_type=2,
        unique_award_key='NON-NULL-SUB',
        fain='NON-FAIN-WITH-DASHES-NULL-SUB',
        awarding_sub_tier_agency_c='5678',
        uei=recipient.uei,
        is_active=True,
        action_date='2020-01-02',
        cfda_number=cfda_2.program_number
    )
    grant_non_null_sub = FSRSGrantFactory(
        id=2,
        fain=fabs_non_null_sub.fain.replace('-', ''),
        federal_agency_id=None,
        awardee_address_country=int_country.country_code_2_char,
        principle_place_country=dom_country.country_code,
        uei_number=recipient.uei,
        parent_uei=parent_recipient.uei,
        cfda_numbers='00.001 CFDA 1; 00.002 CFDA 2',
        obligation_date=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_grant_non_null_sub = FSRSSubgrantFactory(
        parent=grant_non_null_sub,
        awardee_address_country=dom_country.country_code,
        awardee_address_zip=dom_zip.zip5,
        principle_place_country=int_country.country_code_2_char,
        parent_uei=parent_recipient.uei,
        uei_number=recipient.uei,
        subaward_date=datetime.now()
    )
    # FABS Aggregate award
    fabs_agg = PublishedFABSFactory(
        submission_id=sub.submission_id,
        record_type=1,
        unique_award_key='AGG',
        fain='AGG-FAIN-WITH-DASHES',
        awarding_sub_tier_agency_c='1234',
        uei=recipient.uei,
        is_active=True,
        action_date='2020-01-01',
        cfda_number=cfda_1.program_number
    )
    fabs_agg_2 = PublishedFABSFactory(
        submission_id=sub.submission_id,
        record_type=1,
        unique_award_key='AGG',
        fain='AGG-FAIN-WITH-DASHES',
        awarding_sub_tier_agency_c='1234',
        uei=recipient.uei,
        is_active=True,
        action_date='2020-01-02',
        cfda_number=cfda_2.program_number
    )
    grant_agg = FSRSGrantFactory(
        id=1,
        fain=fabs_agg.fain.replace('-', ''),
        federal_agency_id='1234',
        awardee_address_country=int_country.country_code_2_char,
        principle_place_country=dom_country.country_code,
        uei_number=recipient.uei,
        parent_uei=parent_recipient.uei,
        cfda_numbers='00.003 CFDA 3',
        obligation_date=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_grant_agg = FSRSSubgrantFactory(
        parent=grant_agg,
        awardee_address_country=dom_country.country_code,
        awardee_address_zip=dom_zip.zip5,
        principle_place_country=int_country.country_code,
        parent_uei=parent_recipient.uei.lower(),
        uei_number=recipient.uei,
        subaward_date=datetime.now()
    )
    sess.add_all([sub, fabs_non_pop_subtier, fabs_non_pop_subtier_2, grant_non_pop_subtier, sub_grant_non_pop_subtier,
                  fabs_non_null_sub, fabs_non_null_sub_2, grant_non_null_sub, sub_grant_non_null_sub, fabs_agg,
                  fabs_agg_2, grant_agg, sub_grant_agg])
    sess.commit()

    # Grouping the values needed for tests
    fabs_grouped = {}
    all_fabs = [fabs_non_pop_subtier, fabs_non_pop_subtier_2, fabs_non_null_sub, fabs_non_null_sub_2, fabs_agg,
                fabs_agg_2]
    for fabs in all_fabs:
        if fabs.fain not in fabs_grouped.keys():
            fabs_grouped[fabs.fain] = {'award_amount': fabs.federal_action_obligation,
                                       'cfda_num': fabs.cfda_number,
                                       'cfda_title': cfda_1.program_title}
        else:
            fabs_grouped[fabs.fain]['award_amount'] += fabs.federal_action_obligation
            fabs_grouped[fabs.fain]['cfda_num'] += ', ' + fabs.cfda_number
            fabs_grouped[fabs.fain]['cfda_title'] += ', ' + cfda_2.program_title

    # Gather the sql
    populate_subaward_table(sess, 'grant_service', ids=[grant_agg.id, grant_non_pop_subtier.id, grant_non_null_sub.id])

    # Get the records
    grants_results = sess.query(Subaward).order_by(Subaward.prime_id).all()
    assert len(grants_results) == 3

    created_at = updated_at = grants_results[0].created_at

    # Expected Results
    # Note: Aggregates should not be linked
    assert compare_grant_results(grants_results[0], fabs_agg, fabs_agg_2, fabs_grouped[fabs_agg.fain], grant_agg,
                                 sub_grant_agg, parent_recipient, recipient, dom_country, dom_zip, dom_county,
                                 int_country, created_at, updated_at) is False
    # Note: If federal_agency_id is blank, no link should happen and FSRS data needs to be updated
    assert compare_grant_results(grants_results[1], fabs_non_null_sub, fabs_non_null_sub_2,
                                 fabs_grouped[fabs_non_null_sub.fain], grant_non_null_sub, sub_grant_non_null_sub,
                                 parent_recipient, recipient, dom_country, dom_zip, dom_county, int_country, created_at,
                                 updated_at) is False
    assert compare_grant_results(grants_results[2], fabs_non_pop_subtier, fabs_non_pop_subtier_2,
                                 fabs_grouped[fabs_non_pop_subtier.fain], grant_non_pop_subtier,
                                 sub_grant_non_pop_subtier, parent_recipient, recipient, dom_country, dom_zip,
                                 dom_county, int_country, created_at, updated_at) is True


def test_fix_broken_links(database, monkeypatch):
    """ Ensure that fix_broken_links works as intended """

    # Setup - create awards, procurements/grants, subawards
    sess = database.session
    sess.query(Subaward).delete(synchronize_session=False)
    sess.commit()

    parent_recipient, recipient, dom_country, dom_zip, dom_county, int_country, cfda_1, cfda_2 = reference_data(sess)
    min_date = '2019-06-06'
    award_updated_at = '2019-06-07'

    # Setup - Grants
    sub = SubmissionFactory(submission_id=1)
    sub2 = SubmissionFactory(submission_id=2)
    sub3 = SubmissionFactory(submission_id=3)
    fabs_non_pop_subtier = PublishedFABSFactory(
        submission_id=sub.submission_id,
        awarding_sub_tier_agency_c='1234',
        record_type=2,
        unique_award_key='NON-POP-SUB',
        fain='NON-FAIN-WITH-DASHES-POP-SUB',
        uei=recipient.uei,
        is_active=True,
        updated_at=award_updated_at,
        action_date='2020-01-01',
        cfda_number=cfda_1.program_number
    )
    fabs_non_pop_subtier_2 = PublishedFABSFactory(
        submission_id=sub.submission_id,
        awarding_sub_tier_agency_c='1234',
        record_type=2,
        unique_award_key='NON-POP-SUB',
        fain='NON-FAIN-WITH-DASHES-POP-SUB',
        uei=recipient.uei,
        is_active=True,
        updated_at=award_updated_at,
        action_date='2020-01-02',
        cfda_number=cfda_2.program_number
    )
    grant_non_pop_subtier = FSRSGrantFactory(
        id=4,
        fain=fabs_non_pop_subtier.fain.replace('-', ''),
        federal_agency_id='1234',
        awardee_address_country=int_country.country_code,
        principle_place_country=dom_country.country_code,
        uei_number=recipient.uei,
        parent_uei=parent_recipient.uei,
        cfda_numbers='00.001 CFDA 1; 00.002 CFDA 2',
        obligation_date=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_grant_non_pop_subtier = FSRSSubgrantFactory(
        parent=grant_non_pop_subtier,
        awardee_address_country=dom_country.country_code,
        awardee_address_zip=dom_zip.zip5,
        principle_place_country=int_country.country_code_2_char,
        parent_uei=parent_recipient.uei,
        uei_number=recipient.uei.lower(),
        subaward_date=datetime.now()
    )

    fabs_non_null_subtier = PublishedFABSFactory(
        submission_id=sub.submission_id,
        awarding_sub_tier_agency_c='5678',
        record_type=2,
        unique_award_key='NON-NULL-SUB',
        fain='NON-FAIN-WITH-DASHES-NULL-SUB',
        uei=recipient.uei,
        is_active=True,
        updated_at=award_updated_at,
        action_date='2020-01-01',
        cfda_number=cfda_1.program_number
    )
    fabs_non_null_subtier_2 = PublishedFABSFactory(
        submission_id=sub.submission_id,
        awarding_sub_tier_agency_c='5678',
        record_type=2,
        unique_award_key='NON-NULL-SUB',
        fain='NON-FAIN-WITH-DASHES-NULL-SUB',
        uei=recipient.uei,
        is_active=True,
        updated_at=award_updated_at,
        action_date='2020-01-02',
        cfda_number=cfda_2.program_number
    )
    grant_non_null_subtier = FSRSGrantFactory(
        id=3,
        fain=fabs_non_null_subtier.fain.replace('-', ''),
        federal_agency_id=None,
        awardee_address_country=int_country.country_code_2_char,
        principle_place_country=dom_country.country_code,
        uei_number=recipient.uei,
        parent_uei=parent_recipient.uei,
        cfda_numbers='00.001 CFDA 1; 00.002 CFDA 2',
        obligation_date=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_grant_non_null_subtier = FSRSSubgrantFactory(
        parent=grant_non_null_subtier,
        awardee_address_country=dom_country.country_code,
        awardee_address_zip=dom_zip.zip5,
        principle_place_country=int_country.country_code_2_char,
        parent_uei=parent_recipient.uei,
        uei_number=recipient.uei,
        subaward_date=datetime.now()
    )

    fabs_non_other = PublishedFABSFactory(
        submission_id=sub2.submission_id,
        awarding_sub_tier_agency_c='1357',
        record_type=2,
        unique_award_key='NON-OTHER',
        fain='NON-FAIN-WITH-DASHES-OTHER',
        uei=recipient.uei,
        is_active=True,
        updated_at=award_updated_at,
        action_date='2020-01-01',
        cfda_number=cfda_1.program_number
    )
    fabs_non_other_2 = PublishedFABSFactory(
        submission_id=sub2.submission_id,
        awarding_sub_tier_agency_c='1357',
        record_type=2,
        unique_award_key='NON-OTHER',
        fain='NON-FAIN-WITH-DASHES-OTHER',
        uei=recipient.uei,
        is_active=True,
        updated_at=award_updated_at,
        action_date='2020-01-02',
        cfda_number=cfda_2.program_number
    )
    fabs_non_other_dup = PublishedFABSFactory(
        submission_id=sub3.submission_id,
        awarding_sub_tier_agency_c='2468',
        record_type=2,
        unique_award_key='NON-OTHER',
        fain='NON-FAIN-WITH-DASHES-OTHER',
        uei=recipient.uei,
        is_active=True,
        updated_at=award_updated_at,
        action_date='2020-01-01',
        cfda_number=cfda_1.program_number
    )
    fabs_non_other_dup_2 = PublishedFABSFactory(
        submission_id=sub3.submission_id,
        awarding_sub_tier_agency_c='2468',
        record_type=2,
        unique_award_key='NON-OTHER',
        fain='NON-FAIN-WITH-DASHES-OTHER',
        uei=recipient.uei,
        is_active=True,
        updated_at=award_updated_at,
        action_date='2020-01-02',
        cfda_number=cfda_2.program_number
    )
    grant_non_other = FSRSGrantFactory(
        id=2,
        fain=fabs_non_other.fain.replace('-', ''),
        federal_agency_id=None,
        awardee_address_country=int_country.country_code,
        principle_place_country=dom_country.country_code,
        uei_number=recipient.uei,
        parent_uei=parent_recipient.uei,
        cfda_numbers='00.001 CFDA 1; 00.002 CFDA 2',
        obligation_date=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_grant_non_other = FSRSSubgrantFactory(
        parent=grant_non_other,
        awardee_address_country=dom_country.country_code,
        awardee_address_zip=dom_zip.zip5,
        principle_place_country=int_country.country_code_2_char,
        parent_uei=parent_recipient.uei,
        uei_number=recipient.uei,
        subaward_date=datetime.now()
    )

    fabs_agg = PublishedFABSFactory(
        submission_id=sub.submission_id,
        awarding_sub_tier_agency_c='1234',
        record_type=1,
        unique_award_key='AGG',
        fain='AGG-FAIN-WITH-DASHES',
        uei=recipient.uei,
        is_active=True,
        updated_at=award_updated_at,
        action_date='2020-01-01',
        cfda_number=cfda_1.program_number
    )
    fabs_agg_2 = PublishedFABSFactory(
        submission_id=sub.submission_id,
        awarding_sub_tier_agency_c='1234',
        record_type=1,
        unique_award_key='AGG',
        fain='AGG-FAIN-WITH-DASHES',
        uei=recipient.uei,
        is_active=True,
        updated_at=award_updated_at,
        action_date='2020-01-02',
        cfda_number=cfda_2.program_number
    )
    grant_agg = FSRSGrantFactory(
        id=1,
        fain=fabs_agg.fain.replace('-', ''),
        federal_agency_id='1234',
        awardee_address_country=int_country.country_code_2_char,
        principle_place_country=dom_country.country_code,
        uei_number=recipient.uei,
        parent_uei=parent_recipient.uei,
        cfda_numbers='00.003 CFDA 3',
        obligation_date=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_grant_agg = FSRSSubgrantFactory(
        parent=grant_agg,
        awardee_address_country=dom_country.country_code,
        awardee_address_zip=dom_zip.zip5,
        principle_place_country=int_country.country_code_2_char,
        parent_uei=parent_recipient.uei,
        uei_number=recipient.uei,
        subaward_date=datetime.now()
    )

    # Setup - Contracts
    d1_awd = DetachedAwardProcurementFactory(
        submission_id=sub.submission_id,
        idv_type=None,
        unique_award_key='AWD',
        piid='AWD-PIID-WITH-DASHES',
        parent_award_id='AWD-PARENT-AWARD-ID-WITH-DASHES',
        updated_at=award_updated_at,
        action_date='2020-01-01',
        legal_entity_country_code=dom_country.country_code
    )
    d1_awd_2 = DetachedAwardProcurementFactory(
        submission_id=sub.submission_id,
        idv_type=None,
        unique_award_key='AWD',
        piid='AWD-PIID-WITH-DASHES',
        parent_award_id='AWD-PARENT-AWARD-ID-WITH-DASHES',
        updated_at=award_updated_at,
        action_date='2020-01-02',
        legal_entity_country_code=dom_country.country_code
    )
    contract_awd = FSRSProcurementFactory(
        id=5,
        contract_number=d1_awd.piid.replace('-', ''),
        idv_reference_number=d1_awd.parent_award_id.replace('-', ''),
        contracting_office_aid=d1_awd.awarding_sub_tier_agency_c,
        company_address_country=dom_country.country_code,
        principle_place_country=int_country.country_code_2_char,
        uei_number=recipient.uei,
        date_signed=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_contract_awd = FSRSSubcontractFactory(
        parent=contract_awd,
        company_address_country=int_country.country_code_2_char,
        principle_place_country=dom_country.country_code,
        principle_place_zip=dom_zip.zip5 + '1234',
        subcontract_date=datetime.now()
    )
    d1_idv = DetachedAwardProcurementFactory(
        submission_id=sub.submission_id,
        idv_type='C',
        unique_award_key='IDV',
        piid='IDV-PIID-WITH-DASHES',
        parent_award_id='IDV-PARENT-AWARD-IDV-WITH-DASHES',
        updated_at=award_updated_at,
        action_date='2020-01-01',
        legal_entity_country_code=dom_country.country_code
    )
    d1_idv_2 = DetachedAwardProcurementFactory(
        submission_id=sub.submission_id,
        idv_type='C',
        unique_award_key='IDV',
        piid='IDV-PIID-WITH-DASHES',
        parent_award_id='IDV-PARENT-AWARD-IDV-WITH-DASHES',
        updated_at=award_updated_at,
        action_date='2020-01-02',
        legal_entity_country_code=dom_country.country_code
    )
    contract_idv = FSRSProcurementFactory(
        id=6,
        contract_number=d1_idv.piid.replace('-', ''),
        idv_reference_number=d1_idv.parent_award_id.replace('-', ''),
        contracting_office_aid=d1_idv.awarding_sub_tier_agency_c,
        company_address_country=dom_country.country_code,
        principle_place_country=int_country.country_code,
        uei_number=recipient.uei,
        date_signed=datetime.now(),
        date_submitted=datetime(2019, 5, 30, 16, 25, 12, 34)
    )
    sub_contract_idv = FSRSSubcontractFactory(
        parent=contract_idv,
        company_address_country=int_country.country_code_2_char,
        principle_place_country=dom_country.country_code,
        principle_place_zip=dom_zip.zip5 + '1234',
        subcontract_date=datetime.now()
    )

    # Note: not including d1/fabs data
    sess.add_all([sub, contract_awd, sub_contract_awd, contract_idv, sub_contract_idv])
    sess.add_all([sub, grant_non_pop_subtier, sub_grant_non_pop_subtier, grant_non_null_subtier,
                  sub_grant_non_null_subtier, grant_non_other, sub_grant_non_other, grant_agg, sub_grant_agg])
    sess.commit()

    # Grouping the values needed for tests
    fabs_grouped = {}
    all_fabs = [fabs_non_pop_subtier, fabs_non_pop_subtier_2, fabs_non_null_subtier, fabs_non_null_subtier_2, fabs_agg,
                fabs_agg_2, fabs_non_other, fabs_non_other_2, fabs_non_other_dup, fabs_non_other_dup_2]
    for fabs in all_fabs:
        if fabs.fain not in fabs_grouped.keys():
            fabs_grouped[fabs.fain] = {'award_amount': fabs.federal_action_obligation,
                                       'cfda_num': fabs.cfda_number,
                                       'cfda_title': cfda_1.program_title}
        else:
            fabs_grouped[fabs.fain]['award_amount'] += fabs.federal_action_obligation
            fabs_grouped[fabs.fain]['cfda_num'] += ', ' + fabs.cfda_number
            fabs_grouped[fabs.fain]['cfda_title'] += ', ' + cfda_2.program_title

    populate_subaward_table(sess, 'procurement_service', ids=[contract_awd.id, contract_idv.id])
    populate_subaward_table(sess, 'grant_service', ids=[grant_agg.id, grant_non_pop_subtier.id,
                                                        grant_non_null_subtier.id, grant_non_other.id])

    contracts_results = sess.query(Subaward).order_by(Subaward.id).\
        filter(Subaward.subaward_type == 'sub-contract').all()
    grants_results = sess.query(Subaward).order_by(Subaward.id).\
        filter(Subaward.subaward_type == 'sub-grant').all()
    original_ids = [result.id for result in contracts_results + grants_results]

    grant_created_at = grant_updated_at = grants_results[0].created_at
    contract_created_at = contract_updated_at = contracts_results[0].created_at

    # Expected Results - should be False as the award isn't provided
    assert compare_contract_results(contracts_results[0], d1_awd, contract_awd, sub_contract_awd, dom_country, dom_zip,
                                    dom_county, int_country, contract_created_at, contract_updated_at) is False
    assert compare_contract_results(contracts_results[1], d1_idv, contract_idv, sub_contract_idv, dom_country, dom_zip,
                                    dom_county, int_country, contract_created_at, contract_updated_at) is False

    assert compare_grant_results(grants_results[0], fabs_agg, fabs_agg_2, fabs_grouped[fabs_agg.fain], grant_agg,
                                 sub_grant_agg, parent_recipient, recipient, dom_country, dom_zip, dom_county,
                                 int_country, grant_created_at, grant_updated_at) is False
    assert compare_grant_results(grants_results[1], fabs_non_other, fabs_non_other_2, fabs_grouped[fabs_non_other.fain],
                                 grant_non_other, sub_grant_non_other, parent_recipient, recipient, dom_country,
                                 dom_zip, dom_county, int_country, grant_created_at, grant_updated_at) is False
    assert compare_grant_results(grants_results[2], fabs_non_null_subtier, fabs_non_null_subtier_2,
                                 fabs_grouped[fabs_non_null_subtier.fain], grant_non_null_subtier,
                                 sub_grant_non_null_subtier, parent_recipient, recipient, dom_country, dom_zip,
                                 dom_county, int_country, grant_created_at, grant_updated_at) is False
    assert compare_grant_results(grants_results[3], fabs_non_pop_subtier, fabs_non_pop_subtier_2,
                                 fabs_grouped[fabs_non_pop_subtier.fain], grant_non_pop_subtier,
                                 sub_grant_non_pop_subtier, parent_recipient, recipient, dom_country, dom_zip,
                                 dom_county, int_country, grant_created_at, grant_updated_at) is False

    # now add the awards and fix the broken links
    sess.add_all([d1_awd, d1_awd_2, d1_idv, d1_idv_2, fabs_non_null_subtier, fabs_non_null_subtier_2,
                  fabs_non_pop_subtier, fabs_non_pop_subtier_2, fabs_non_other, fabs_non_other_2, fabs_non_other_dup,
                  fabs_non_other_dup_2, fabs_agg, fabs_agg_2])
    sess.commit()

    updated_proc_count = fix_broken_links(sess, 'procurement_service', min_date=min_date)
    updated_grant_count = fix_broken_links(sess, 'grant_service', min_date=min_date)

    assert updated_proc_count == 2
    # Note: Aggregates and blank federal_agency_ids should still not be linked, so 1 and not 3
    assert updated_grant_count == 1

    contracts_results = sess.query(Subaward).order_by(Subaward.unique_award_key).\
        filter(Subaward.subaward_type == 'sub-contract').all()
    grants_results = sess.query(Subaward).order_by(Subaward.award_id).\
        filter(Subaward.subaward_type == 'sub-grant').all()
    updated_ids = [result.id for result in contracts_results + grants_results]

    contract_created_at = contracts_results[0].created_at
    contract_updated_at = contracts_results[0].updated_at
    grant_created_at = grants_results[0].created_at
    grant_updated_at = grants_results[0].updated_at

    # Expected Results - should now be True as the award is now available
    assert compare_contract_results(contracts_results[0], d1_awd, contract_awd, sub_contract_awd, dom_country, dom_zip,
                                    dom_county, int_country, contract_created_at, contract_updated_at) is True
    assert compare_contract_results(contracts_results[1], d1_idv, contract_idv, sub_contract_idv, dom_country, dom_zip,
                                    dom_county, int_country, contract_created_at, contract_updated_at) is True
    assert compare_grant_results(grants_results[0], fabs_agg, fabs_agg_2, fabs_grouped[fabs_agg.fain], grant_agg,
                                 sub_grant_agg, parent_recipient, recipient, dom_country, dom_zip, dom_county,
                                 int_country, grant_created_at, grant_updated_at) is False
    assert compare_grant_results(grants_results[1], fabs_non_null_subtier, fabs_non_null_subtier_2,
                                 fabs_grouped[fabs_non_null_subtier.fain], grant_non_null_subtier,
                                 sub_grant_non_null_subtier, parent_recipient, recipient, dom_country, dom_zip,
                                 dom_county, int_country, grant_created_at, grant_updated_at) is False
    assert compare_grant_results(grants_results[2], fabs_non_other, fabs_non_other_2, fabs_grouped[fabs_non_other.fain],
                                 grant_non_other, sub_grant_non_other, parent_recipient, recipient, dom_country,
                                 dom_zip, dom_county, int_country, grant_created_at, grant_updated_at) is False
    assert compare_grant_results(grants_results[3], fabs_non_pop_subtier, fabs_non_pop_subtier_2,
                                 fabs_grouped[fabs_non_pop_subtier.fain], grant_non_pop_subtier,
                                 sub_grant_non_pop_subtier, parent_recipient, recipient, dom_country, dom_zip,
                                 dom_county, int_country, grant_created_at, grant_updated_at) is True

    # Ensuring only updates occurred, no deletes/inserts
    assert set(original_ids) == set(updated_ids)
