from datetime import datetime
from collections import namedtuple

from dataactcore.scripts.pipeline.populate_subaward_table import populate_subaward_table, fix_broken_links
from dataactbroker.helpers.generic_helper import fy
from dataactcore.models.fsrs import Subaward
from tests.unit.dataactcore.factories.fsrs import (SAMSubcontractFactory, SAMSubgrantFactory)
from tests.unit.dataactcore.factories.staging import PublishedFABSFactory, DetachedAwardProcurementFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.domain import (SAMRecipientFactory, AssistanceListingFactory, CountryCodeFactory,
                                                     CountyCodeFactory, ZipsFactory, ZipsGroupedFactory)


Location = namedtuple('Location', ['country', 'zip', 'county'])


def reference_data(sess):
    reference_data_dict = {
        'sub_recipient': SAMRecipientFactory(uei='987654321CCC', legal_business_name='TEST SUB RECIPIENT',
                                             awardee_or_recipient_uniqu='987654321'),
        'sub_parent_recipient': SAMRecipientFactory(uei='987654322PPP', legal_business_name='TEST SUB PARENT RECIPIENT',
                                                    awardee_or_recipient_uniqu='987654322'),
        'prime_recipient': SAMRecipientFactory(uei='123456789CCC', legal_business_name='TEST PRIME RECIPIENT',
                                               business_types=['TYPE 1', 'TYPE 2', 'TYPE 3'],
                                               awardee_or_recipient_uniqu='123456789'),
        'dom_country': CountryCodeFactory(country_code='USA', country_code_2_char='US', country_name='UNITED STATES'),
        'dom_zip5': ZipsGroupedFactory(zip5='12345', county_number='543', state_abbreviation='VA'),
        'dom_zip9': ZipsFactory(zip5='12345', zip_last4='1234', county_number='987', state_abbreviation='VA'),
        'dom_county_zip5': CountyCodeFactory(county_number='543', county_name='DOM COUNTY ZIP5', state_code='VA'),
        'dom_county_zip9': CountyCodeFactory(county_number='987', county_name='DOM COUNTY ZIP9', state_code='VA'),
        'int_country': CountryCodeFactory(country_code='INT', country_code_2_char='IT', country_name='INTERNATIONAL'),
        'assistance_listing_1': AssistanceListingFactory(program_number='10.000', program_title='TEST NUMBER 1'),
        'assistance_listing_2': AssistanceListingFactory(program_number='20.000', program_title='TEST NUMBER 2'),
    }
    sess.add_all(list(reference_data_dict.values()))
    return reference_data_dict


def compare_contract_results(sub, fpds_base, fpds_latest, sub_contract, created_at, updated_at, sub_recipient=None,
                             sub_parent_recipient=None, prime_recipient=None, debug=True):
    """ Helper function for contract results """
    attr = {
        'created_at': created_at,
        'updated_at': updated_at,
        'id': sub.id,

        'unique_award_key': fpds_latest.unique_award_key.upper(),
        'award_id': fpds_latest.piid,
        'parent_award_id': fpds_latest.parent_award_id,
        'award_amount': fpds_latest.total_obligated_amount,
        'action_date': str(fpds_base.action_date),
        'fy': 'FY{}'.format(fy(fpds_base.action_date)),
        'awarding_agency_code': fpds_latest.awarding_agency_code,
        'awarding_agency_name': fpds_latest.awarding_agency_name,
        'awarding_sub_tier_agency_c': fpds_latest.awarding_sub_tier_agency_c,
        'awarding_sub_tier_agency_n': fpds_latest.awarding_sub_tier_agency_n,
        'awarding_office_code': fpds_latest.awarding_office_code,
        'awarding_office_name': fpds_latest.awarding_office_name,
        'funding_agency_code': fpds_latest.funding_agency_code,
        'funding_agency_name': fpds_latest.funding_agency_name,
        'funding_sub_tier_agency_co': fpds_latest.funding_sub_tier_agency_co,
        'funding_sub_tier_agency_na': fpds_latest.funding_sub_tier_agency_na,
        'funding_office_code': fpds_latest.funding_office_code,
        'funding_office_name': fpds_latest.funding_office_name,
        'awardee_or_recipient_uniqu': fpds_latest.awardee_or_recipient_uniqu,
        'awardee_or_recipient_uei': fpds_latest.awardee_or_recipient_uei,
        'awardee_or_recipient_legal': fpds_latest.awardee_or_recipient_legal,
        'dba_name': fpds_latest.vendor_doing_as_business_n,
        'ultimate_parent_unique_ide': fpds_latest.ultimate_parent_unique_ide,
        'ultimate_parent_uei': fpds_latest.ultimate_parent_uei,
        'ultimate_parent_legal_enti': fpds_latest.ultimate_parent_legal_enti,
        'legal_entity_country_code': fpds_latest.legal_entity_country_code.upper(),
        'legal_entity_country_name': fpds_latest.legal_entity_country_name,
        'legal_entity_address_line1': fpds_latest.legal_entity_address_line1,
        'legal_entity_city_name': fpds_latest.legal_entity_city_name,
        'legal_entity_state_code': fpds_latest.legal_entity_state_code,
        'legal_entity_state_name': fpds_latest.legal_entity_state_descrip,
        'legal_entity_zip': (fpds_latest.legal_entity_zip4
                             if fpds_latest.legal_entity_country_code.upper() == 'USA' else None),
        "legal_entity_county_code": fpds_latest.legal_entity_county_code,
        "legal_entity_county_name": fpds_latest.legal_entity_county_name,
        'legal_entity_congressional': fpds_latest.legal_entity_congressional,
        'legal_entity_foreign_posta': (fpds_latest.legal_entity_zip4
                                       if fpds_latest.legal_entity_country_code.upper() != 'USA' else None),
        'business_types': ','.join(prime_recipient.business_types) if prime_recipient.business_types else None,
        'place_of_perform_city_name': fpds_latest.place_of_perform_city_name,
        'place_of_perform_state_code': fpds_latest.place_of_performance_state,
        'place_of_perform_state_name': fpds_latest.place_of_perfor_state_desc,
        'place_of_performance_zip': fpds_latest.place_of_performance_zip4a,
        'place_of_performance_county_code': fpds_latest.place_of_perform_county_co,
        'place_of_performance_county_name': fpds_latest.place_of_perform_county_na,
        'place_of_perform_congressio': fpds_latest.place_of_performance_congr,
        'place_of_perform_country_co': fpds_latest.place_of_perform_country_c.upper(),
        'place_of_perform_country_na': fpds_latest.place_of_perf_country_desc,
        'award_description': fpds_base.award_description,
        'naics': fpds_latest.naics,
        'naics_description': fpds_latest.naics_description,
        'assistance_listing_numbers': None,
        'assistance_listing_titles': None,

        'prime_id': None,
        'report_type': None,
        'transaction_type': None,
        'program_title': None,
        'contract_agency_code': sub_contract.contract_agency_code,
        'contract_idv_agency_code': sub_contract.contract_idv_agency_code,
        'grant_funding_agency_id': None,
        'grant_funding_agency_name': None,
        'federal_agency_name': None,
        'treasury_symbol': None,
        'dunsplus4': None,
        'recovery_model_q1': None,
        'recovery_model_q2': None,
        'compensation_q1': None,
        'compensation_q2': None,
        'high_comp_officer1_full_na': fpds_latest.high_comp_officer1_full_na,
        'high_comp_officer1_amount': fpds_latest.high_comp_officer1_amount,
        'high_comp_officer2_full_na': fpds_latest.high_comp_officer2_full_na,
        'high_comp_officer2_amount': fpds_latest.high_comp_officer2_amount,
        'high_comp_officer3_full_na': fpds_latest.high_comp_officer3_full_na,
        'high_comp_officer3_amount': fpds_latest.high_comp_officer3_amount,
        'high_comp_officer4_full_na': fpds_latest.high_comp_officer4_full_na,
        'high_comp_officer4_amount': fpds_latest.high_comp_officer4_amount,
        'high_comp_officer5_full_na': fpds_latest.high_comp_officer5_full_na,
        'high_comp_officer5_amount': fpds_latest.high_comp_officer5_amount,
        'place_of_perform_street': None,

        'subaward_type': 'sub-contract',
        'internal_id': sub_contract.subaward_report_number,
        'date_submitted': sub_contract.date_submitted.strftime('%Y-%m-%d'),
        'subaward_report_year': sub_contract.date_submitted.strftime('%Y'),
        'subaward_report_month': sub_contract.date_submitted.strftime('%m'),
        'subaward_number': sub_contract.award_number,
        'subaward_amount': sub_contract.award_amount,
        'sub_action_date': str(sub_contract.action_date),
        'sub_awardee_or_recipient_uniqu': sub_recipient.awardee_or_recipient_uniqu,
        'sub_awardee_or_recipient_uei': sub_contract.uei,
        'sub_awardee_or_recipient_legal': sub_contract.legal_business_name,
        'sub_dba_name': sub_contract.dba_name,
        'sub_ultimate_parent_unique_ide': sub_parent_recipient.awardee_or_recipient_uniqu,
        'sub_ultimate_parent_uei': sub_contract.parent_uei,
        'sub_ultimate_parent_legal_enti': sub_contract.parent_legal_business_name,
        'sub_legal_entity_country_code': sub_contract.legal_entity_country_code,
        'sub_legal_entity_country_name': sub_contract.legal_entity_country_name,
        'sub_legal_entity_address_line1': sub_contract.legal_entity_address_line1,
        'sub_legal_entity_city_name': sub_contract.legal_entity_city_name,
        'sub_legal_entity_state_code': (sub_contract.legal_entity_state_code
                                        if sub_contract.legal_entity_state_code.upper() != 'ZZ' else None),
        'sub_legal_entity_state_name': (sub_contract.legal_entity_state_name
                                        if sub_contract.legal_entity_state_code.upper() != 'ZZ' else None),
        'sub_legal_entity_zip': (sub_contract.legal_entity_zip_code
                                 if sub_contract.legal_entity_country_code.upper() == 'USA' else None),
        'sub_legal_entity_county_code': None,
        'sub_legal_entity_county_name': None,
        'sub_legal_entity_congressional': sub_contract.legal_entity_congressional,
        'sub_legal_entity_foreign_posta': (sub_contract.legal_entity_zip_code
                                           if sub_contract.legal_entity_country_code.upper() != 'USA' else None),
        'sub_business_types': (','.join(sub_contract.business_types_names)
                               if sub_contract.business_types_names else None),
        'sub_place_of_perform_city_name': sub_contract.ppop_city_name,
        'sub_place_of_perform_state_code': (sub_contract.ppop_state_code
                                            if sub_contract.ppop_state_code.upper() != 'ZZ' else None),
        'sub_place_of_perform_state_name': (sub_contract.ppop_state_name
                                            if sub_contract.ppop_state_code.upper() != 'ZZ' else None),
        'sub_place_of_performance_zip': sub_contract.ppop_zip_code,
        'sub_place_of_performance_county_code': None,
        'sub_place_of_performance_county_name': None,
        'sub_place_of_perform_congressio': sub_contract.ppop_congressional_district,
        'sub_place_of_perform_country_co': sub_contract.ppop_country_code,
        'sub_place_of_perform_country_na': sub_contract.ppop_country_name,
        'subaward_description': sub_contract.description,
        'sub_high_comp_officer1_full_na': sub_contract.high_comp_officer1_full_na,
        'sub_high_comp_officer1_amount': sub_contract.high_comp_officer1_amount,
        'sub_high_comp_officer2_full_na': sub_contract.high_comp_officer2_full_na,
        'sub_high_comp_officer2_amount': sub_contract.high_comp_officer2_amount,
        'sub_high_comp_officer3_full_na': sub_contract.high_comp_officer3_full_na,
        'sub_high_comp_officer3_amount': sub_contract.high_comp_officer3_amount,
        'sub_high_comp_officer4_full_na': sub_contract.high_comp_officer4_full_na,
        'sub_high_comp_officer4_amount': sub_contract.high_comp_officer4_amount,
        'sub_high_comp_officer5_full_na': sub_contract.high_comp_officer5_full_na,
        'sub_high_comp_officer5_amount': sub_contract.high_comp_officer5_amount,

        'sub_id': sub_contract.subaward_report_id,
        'sub_parent_id': None,
        'sub_federal_agency_id': fpds_latest.awarding_sub_tier_agency_c,
        'sub_federal_agency_name': fpds_latest.awarding_sub_tier_agency_n,
        'sub_funding_agency_id': fpds_latest.funding_sub_tier_agency_co,
        'sub_funding_agency_name': fpds_latest.funding_sub_tier_agency_na,
        'sub_funding_office_id': None,
        'sub_funding_office_name': None,
        'sub_naics': fpds_latest.naics,
        'sub_assistance_listing_numbers': None,
        'sub_dunsplus4': None,
        'sub_recovery_subcontract_amt': None,
        'sub_recovery_model_q1': None,
        'sub_recovery_model_q2': None,
        'sub_compensation_q1': None,
        'sub_compensation_q2': None,
        'sub_place_of_perform_street': sub_contract.ppop_address_line1
    }
    compare = (attr.items() <= sub.__dict__.items())

    if debug and not compare:
        print(sorted(attr.items()))
        print(sorted(sub.__dict__.items()))
    return compare


def test_generate_f_file_queries_contracts(database):
    """ generate_f_file_queries should provide queries representing halves of F file data related to a submission
        This will cover contracts records.
    """
    sess = database.session
    sess.query(Subaward).delete(synchronize_session=False)
    sess.commit()

    ref_data_cont = reference_data(sess)
    # Setting the test location data - can be varied
    ref_data_cont['prime_ppop'] = Location(country=ref_data_cont['dom_country'], zip=ref_data_cont['dom_zip5'],
                                           county=ref_data_cont['dom_county_zip5'])
    ref_data_cont['prime_le'] = Location(country=ref_data_cont['int_country'], zip=None, county=None)
    ref_data_cont['sub_ppop'] = Location(country=ref_data_cont['dom_country'], zip=ref_data_cont['dom_zip9'],
                                         county=ref_data_cont['dom_county_zip9'])
    ref_data_cont['sub_le'] = Location(country=ref_data_cont['int_country'], zip=None, county=None)

    sub = SubmissionFactory(submission_id=1)
    fpds_base = DetachedAwardProcurementFactory(
        submission_id=sub.submission_id,
        action_date='2025-03-15',
    )
    fpds_latest = DetachedAwardProcurementFactory(
        submission_id=sub.submission_id,
        unique_award_key=fpds_base.unique_award_key,
        action_date='2025-03-16',
        awardee_or_recipient_uei=ref_data_cont['prime_recipient'].uei,
        legal_entity_country_code=ref_data_cont['prime_le'].country.country_code,
        legal_entity_zip4=ref_data_cont['prime_le'].zip.zip5 if ref_data_cont['prime_le'].zip else None,
        legal_entity_county_code=(ref_data_cont['prime_le'].county.county_number
                                  if ref_data_cont['prime_le'].county else None),
        place_of_perform_country_c=ref_data_cont['prime_ppop'].country.country_code,
        place_of_performance_zip4a=(ref_data_cont['prime_ppop'].zip.zip5
                                    if ref_data_cont['prime_ppop'].zip else None),
        place_of_perform_county_co=(ref_data_cont['prime_ppop'].county.county_number
                                    if ref_data_cont['prime_ppop'].county else None),
    )
    sub_contract = SAMSubcontractFactory(
        unique_award_key=fpds_base.unique_award_key,
        uei=ref_data_cont['sub_recipient'].uei,
        parent_uei=ref_data_cont['sub_parent_recipient'].uei,
        date_submitted=datetime(2025, 3, 17, 16, 25, 12, 34),
        legal_entity_country_code=ref_data_cont['sub_le'].country.country_code,
        legal_entity_zip_code=ref_data_cont['sub_le'].zip.zip5 if ref_data_cont['sub_le'].zip else None,
        ppop_country_code=ref_data_cont['sub_ppop'].country.country_code,
        ppop_zip_code=(ref_data_cont['sub_ppop'].zip.zip5
                       if ref_data_cont['sub_ppop'].zip else None),
        action_date=datetime.now()
    )

    sess.add_all([sub, fpds_base, fpds_latest, sub_contract])
    sess.commit()

    populate_subaward_table(sess, 'contract', report_nums=[sub_contract.subaward_report_number])

    contracts_results = sess.query(Subaward).order_by(Subaward.internal_id).all()
    assert len(contracts_results) == 1

    created_at = updated_at = contracts_results[0].created_at
    drop_ref_fields = ['dom_country', 'dom_zip5', 'dom_zip9', 'dom_county_zip5', 'dom_county_zip9', 'int_country',
                       'prime_ppop', 'prime_le', 'sub_ppop', 'sub_le', 'assistance_listing_1', 'assistance_listing_2']
    for drop_ref_field in drop_ref_fields:
        ref_data_cont.pop(drop_ref_field)

    assert compare_contract_results(contracts_results[0], fpds_base, fpds_latest, sub_contract, created_at, updated_at,
                                    **ref_data_cont) is True


def compare_grant_results(sub, fabs_base, fabs_latest, fabs_grouped, sub_grant, created_at, updated_at,
                          sub_recipient=None, sub_parent_recipient=None, prime_recipient=None, debug=True):
    """ Helper function for grant results """
    attr = {
        'created_at': created_at,
        'updated_at': updated_at,
        'id': sub.id,

        'unique_award_key': fabs_latest.unique_award_key.upper(),
        'award_id': fabs_latest.fain,
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
        'dba_name': prime_recipient.dba_name,
        'ultimate_parent_unique_ide': fabs_latest.ultimate_parent_unique_ide,
        'ultimate_parent_uei': fabs_latest.ultimate_parent_uei,
        'ultimate_parent_legal_enti': fabs_latest.ultimate_parent_legal_enti,
        'legal_entity_country_code': fabs_latest.legal_entity_country_code,
        'legal_entity_country_name': fabs_latest.legal_entity_country_name,
        'legal_entity_address_line1': fabs_latest.legal_entity_address_line1,
        'legal_entity_city_name': fabs_latest.legal_entity_city_name,
        'legal_entity_state_code': fabs_latest.legal_entity_state_code,
        'legal_entity_state_name': fabs_latest.legal_entity_state_name,
        'legal_entity_zip': fabs_latest.legal_entity_zip5 if fabs_latest.legal_entity_country_code == 'USA' else None,
        'legal_entity_county_code': fabs_latest.legal_entity_county_code,
        'legal_entity_county_name': fabs_latest.legal_entity_county_name,
        'legal_entity_congressional': fabs_latest.legal_entity_congressional,
        'legal_entity_foreign_posta': (fabs_latest.legal_entity_foreign_posta
                                       if fabs_latest.legal_entity_country_code != 'USA' else None),
        'business_types': fabs_latest.business_types_desc,
        'place_of_perform_city_name': fabs_latest.place_of_performance_city,
        'place_of_perform_state_code': fabs_latest.place_of_perfor_state_code,
        'place_of_perform_state_name': fabs_latest.place_of_perform_state_nam,
        'place_of_performance_zip': fabs_latest.place_of_performance_zip4a,
        'place_of_performance_county_code': fabs_latest.place_of_perform_county_co,
        'place_of_performance_county_name': fabs_latest.place_of_perform_county_na,
        'place_of_perform_congressio': fabs_latest.place_of_performance_congr,
        'place_of_perform_country_co': fabs_latest.place_of_perform_country_c.upper(),
        'place_of_perform_country_na': fabs_latest.place_of_perform_country_n,
        'award_description': fabs_base.award_description,
        'naics': None,
        'naics_description': None,
        # assistance_listing_nums/names check below

        'prime_id': None,
        'report_type': None,
        'transaction_type': None,
        'program_title': None,
        'contract_agency_code': None,
        'contract_idv_agency_code': None,
        'grant_funding_agency_id': fabs_latest.funding_sub_tier_agency_co,
        'grant_funding_agency_name': fabs_latest.funding_sub_tier_agency_na,
        'federal_agency_name': fabs_latest.awarding_sub_tier_agency_c,
        'treasury_symbol': None,
        'dunsplus4': None,
        'recovery_model_q1': None,
        'recovery_model_q2': None,
        'compensation_q1': None,
        'compensation_q2': None,
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
        'place_of_perform_street': None,

        'subaward_type': 'sub-grant',
        'internal_id': sub_grant.subaward_report_number,
        'date_submitted': sub_grant.date_submitted.strftime('%Y-%m-%d'),
        'subaward_report_year': sub_grant.date_submitted.strftime('%Y'),
        'subaward_report_month': sub_grant.date_submitted.strftime('%m'),
        'subaward_number': sub_grant.award_number,
        'subaward_amount': sub_grant.award_amount,
        'sub_action_date': str(sub_grant.action_date),
        'sub_awardee_or_recipient_uniqu': sub_recipient.awardee_or_recipient_uniqu,
        'sub_awardee_or_recipient_uei': sub_grant.uei,
        'sub_awardee_or_recipient_legal': sub_grant.legal_business_name,
        'sub_dba_name': sub_grant.dba_name,
        'sub_ultimate_parent_unique_ide': sub_parent_recipient.awardee_or_recipient_uniqu,
        'sub_ultimate_parent_uei': sub_grant.parent_uei,
        'sub_ultimate_parent_legal_enti': sub_grant.parent_legal_business_name,
        'sub_legal_entity_country_code': sub_grant.legal_entity_country_code,
        'sub_legal_entity_country_name': sub_grant.legal_entity_country_name,
        'sub_legal_entity_address_line1': sub_grant.legal_entity_address_line1,
        'sub_legal_entity_city_name': sub_grant.legal_entity_city_name,
        'sub_legal_entity_state_code': (sub_grant.legal_entity_state_code
                                        if sub_grant.legal_entity_state_code.upper() != 'ZZ' else None),
        'sub_legal_entity_state_name': (sub_grant.legal_entity_state_name
                                        if sub_grant.legal_entity_state_code.upper() != 'ZZ' else None),
        'sub_legal_entity_zip': (sub_grant.legal_entity_zip_code
                                 if sub_grant.legal_entity_country_code.upper() == 'USA' else None),
        'sub_legal_entity_county_code': None,
        'sub_legal_entity_county_name': None,
        'sub_legal_entity_congressional': sub_grant.legal_entity_congressional,
        'sub_legal_entity_foreign_posta': (sub_grant.legal_entity_zip_code
                                           if sub_grant.legal_entity_country_code.upper() != 'USA' else None),
        'sub_business_types': ','.join(sub_grant.business_types_names) if sub_grant.business_types_names else None,
        'sub_place_of_perform_city_name': sub_grant.ppop_city_name,
        'sub_place_of_perform_state_code': (sub_grant.ppop_state_code
                                            if sub_grant.ppop_state_code.upper() != 'ZZ' else None),
        'sub_place_of_perform_state_name': (sub_grant.ppop_state_name
                                            if sub_grant.ppop_state_code.upper() != 'ZZ' else None),
        'sub_place_of_performance_zip': sub_grant.ppop_zip_code,
        'sub_place_of_performance_county_code': None,
        'sub_place_of_performance_county_name': None,
        'sub_place_of_perform_congressio': sub_grant.ppop_congressional_district,
        'sub_place_of_perform_country_co': sub_grant.ppop_country_code,
        'sub_place_of_perform_country_na': sub_grant.ppop_country_name,
        'subaward_description': sub_grant.description,
        'sub_high_comp_officer1_full_na': sub_grant.high_comp_officer1_full_na,
        'sub_high_comp_officer1_amount': sub_grant.high_comp_officer1_amount,
        'sub_high_comp_officer2_full_na': sub_grant.high_comp_officer2_full_na,
        'sub_high_comp_officer2_amount': sub_grant.high_comp_officer2_amount,
        'sub_high_comp_officer3_full_na': sub_grant.high_comp_officer3_full_na,
        'sub_high_comp_officer3_amount': sub_grant.high_comp_officer3_amount,
        'sub_high_comp_officer4_full_na': sub_grant.high_comp_officer4_full_na,
        'sub_high_comp_officer4_amount': sub_grant.high_comp_officer4_amount,
        'sub_high_comp_officer5_full_na': sub_grant.high_comp_officer5_full_na,
        'sub_high_comp_officer5_amount': sub_grant.high_comp_officer5_amount,

        'sub_id': sub_grant.subaward_report_id,
        'sub_parent_id': None,
        'sub_federal_agency_id': fabs_latest.awarding_sub_tier_agency_c,
        'sub_federal_agency_name': fabs_latest.awarding_sub_tier_agency_n,
        'sub_funding_agency_id': fabs_latest.funding_sub_tier_agency_co,
        'sub_funding_agency_name': fabs_latest.funding_sub_tier_agency_na,
        'sub_funding_office_id': None,
        'sub_funding_office_name': None,
        'sub_naics': None,
        'sub_dunsplus4': None,
        'sub_recovery_subcontract_amt': None,
        'sub_recovery_model_q1': None,
        'sub_recovery_model_q2': None,
        'sub_compensation_q1': None,
        'sub_compensation_q2': None,
        'sub_place_of_perform_street': sub_grant.ppop_address_line1
    }
    normal_compare = (attr.items() <= sub.__dict__.items())

    array_attrs = {
        'assistance_listing_numbers': fabs_grouped['assistance_listing_num'],
        'assistance_listing_titles': fabs_grouped['assistance_listing_title'],
        'sub_assistance_listing_numbers': fabs_grouped['assistance_listing_num'],
    }
    array_compare = True
    for ar_name, ar_value in array_attrs.items():
        if sub.__dict__[ar_name] is not None:
            array_compare &= (sorted(ar_value) == sorted(sub.__dict__[ar_name]))

    compare = normal_compare & array_compare
    if debug and not compare:
        print(sorted(attr.items()))
        print(sorted(sub.__dict__.items()))
    return compare


def test_generate_f_file_queries_assistance(database):
    """ generate_f_file_queries should provide queries representing halves of F file data related to a submission
        This will cover assistance records.
    """
    sess = database.session
    sess.query(Subaward).delete(synchronize_session=False)
    sess.commit()

    ref_data_assist = reference_data(sess)
    # Setting the test location data - can be varied
    ref_data_assist['prime_ppop'] = Location(country=ref_data_assist['dom_country'], zip=ref_data_assist['dom_zip5'],
                                             county=ref_data_assist['dom_county_zip5'])
    ref_data_assist['prime_le'] = Location(country=ref_data_assist['int_country'], zip=None, county=None)
    ref_data_assist['sub_ppop'] = Location(country=ref_data_assist['dom_country'], zip=ref_data_assist['dom_zip9'],
                                           county=ref_data_assist['dom_county_zip9'])
    ref_data_assist['sub_le'] = Location(country=ref_data_assist['int_country'], zip=None, county=None)

    sub = SubmissionFactory(submission_id=1)
    fabs_base = PublishedFABSFactory(
        submission_id=sub.submission_id,
        uei=ref_data_assist['prime_recipient'].uei,
        is_active=True,
        action_date='2025-03-15',
        assistance_listing_number=ref_data_assist['assistance_listing_1'].program_number
    )
    fabs_latest = PublishedFABSFactory(
        submission_id=sub.submission_id,
        unique_award_key=fabs_base.unique_award_key,
        uei=ref_data_assist['prime_recipient'].uei,
        is_active=True,
        action_date='2025-03-16',
        assistance_listing_number=ref_data_assist['assistance_listing_2'].program_number,
        legal_entity_country_code=ref_data_assist['prime_le'].country.country_code,
        legal_entity_zip4=ref_data_assist['prime_le'].zip.zip5 if ref_data_assist['prime_le'].zip else None,
        legal_entity_county_code=(ref_data_assist['prime_le'].county.county_number
                                  if ref_data_assist['prime_le'].county else None),
        place_of_perform_country_c=ref_data_assist['prime_ppop'].country.country_code,
        place_of_performance_zip4a=(ref_data_assist['prime_ppop'].zip.zip5
                                    if ref_data_assist['prime_ppop'].zip else None),
        place_of_perform_county_co=(ref_data_assist['prime_ppop'].county.county_number
                                    if ref_data_assist['prime_ppop'].county else None),
    )
    sub_grant = SAMSubgrantFactory(
        unique_award_key=fabs_base.unique_award_key,
        uei=ref_data_assist['sub_recipient'].uei,
        parent_uei=ref_data_assist['sub_parent_recipient'].uei,
        date_submitted=datetime(2025, 3, 17, 16, 25, 12, 34),
        action_date=datetime.now(),
        legal_entity_country_code=ref_data_assist['sub_le'].country.country_code,
        legal_entity_zip_code=ref_data_assist['sub_le'].zip.zip5 if ref_data_assist['sub_le'].zip else None,
        ppop_country_code=ref_data_assist['sub_ppop'].country.country_code,
        ppop_zip_code=(ref_data_assist['sub_ppop'].zip.zip5
                       if ref_data_assist['sub_ppop'].zip else None),
    )
    sess.add_all([sub, fabs_base, fabs_latest, sub_grant])
    sess.commit()

    fabs_grouped = {}
    all_fabs = [fabs_base, fabs_latest]
    for fabs in all_fabs:
        if fabs.unique_award_key not in fabs_grouped.keys():
            fabs_grouped[fabs.unique_award_key] = {'award_amount': fabs.federal_action_obligation,
                                                   'assistance_listing_num': fabs.assistance_listing_number,
                                                   'assistance_listing_title':
                                                       ref_data_assist['assistance_listing_1'].program_title}
        else:
            fabs_grouped[fabs.unique_award_key]['award_amount'] += fabs.federal_action_obligation
            fabs_grouped[fabs.unique_award_key]['assistance_listing_num'] += ', ' + fabs.assistance_listing_number
            fabs_grouped[fabs.unique_award_key]['assistance_listing_title'] += \
                ', ' + ref_data_assist['assistance_listing_2'].program_title

    populate_subaward_table(sess, 'assistance', report_nums=[sub_grant.subaward_report_number])

    grants_results = sess.query(Subaward).order_by(Subaward.internal_id).all()
    assert len(grants_results) == 1

    created_at = updated_at = grants_results[0].created_at

    drop_ref_fields = ['dom_country', 'dom_zip5', 'dom_zip9', 'dom_county_zip5', 'dom_county_zip9', 'int_country',
                       'prime_ppop', 'prime_le', 'sub_ppop', 'sub_le', 'assistance_listing_1', 'assistance_listing_2']
    for drop_ref_field in drop_ref_fields:
        ref_data_assist.pop(drop_ref_field)

    assert compare_grant_results(grants_results[0], fabs_base, fabs_latest, fabs_grouped[fabs_latest.unique_award_key],
                                 sub_grant, created_at, updated_at, **ref_data_assist) is True


def test_fix_broken_links(database):
    """ Ensure that fix_broken_links works as intended """

    sess = database.session
    sess.query(Subaward).delete(synchronize_session=False)
    sess.commit()

    ref_data = reference_data(sess)
    # Setting the test location data - can be varied
    ref_data['prime_ppop'] = Location(country=ref_data['dom_country'], zip=ref_data['dom_zip5'],
                                      county=ref_data['dom_county_zip5'])
    ref_data['prime_le'] = Location(country=ref_data['int_country'], zip=None, county=None)
    ref_data['sub_ppop'] = Location(country=ref_data['dom_country'], zip=ref_data['dom_zip9'],
                                    county=ref_data['dom_county_zip9'])
    ref_data['sub_le'] = Location(country=ref_data['int_country'], zip=None, county=None)

    # Grants
    fabs_sub = SubmissionFactory(submission_id=1)
    fabs_base = PublishedFABSFactory(
        submission_id=fabs_sub.submission_id,
        uei=ref_data['prime_recipient'].uei,
        is_active=True,
        action_date='2025-03-15',
        assistance_listing_number=ref_data['assistance_listing_1'].program_number
    )
    fabs_latest = PublishedFABSFactory(
        submission_id=fabs_sub.submission_id,
        unique_award_key=fabs_base.unique_award_key,
        uei=ref_data['prime_recipient'].uei,
        is_active=True,
        action_date='2025-03-16',
        assistance_listing_number=ref_data['assistance_listing_2'].program_number,
        legal_entity_country_code=ref_data['prime_le'].country.country_code,
        legal_entity_zip4=ref_data['prime_le'].zip.zip5 if ref_data['prime_le'].zip else None,
        legal_entity_county_code=(ref_data['prime_le'].county.county_number
                                  if ref_data['prime_le'].county else None),
        place_of_perform_country_c=ref_data['prime_ppop'].country.country_code,
        place_of_performance_zip4a=(ref_data['prime_ppop'].zip.zip5
                                    if ref_data['prime_ppop'].zip else None),
        place_of_perform_county_co=(ref_data['prime_ppop'].county.county_number
                                    if ref_data['prime_ppop'].county else None),
    )
    sub_grant = SAMSubgrantFactory(
        unique_award_key=fabs_base.unique_award_key,
        uei=ref_data['sub_recipient'].uei,
        parent_uei=ref_data['sub_parent_recipient'].uei,
        date_submitted=datetime(2025, 3, 17, 16, 25, 12, 34),
        action_date=datetime.now(),
        legal_entity_country_code=ref_data['sub_le'].country.country_code,
        legal_entity_zip_code=ref_data['sub_le'].zip.zip5 if ref_data['sub_le'].zip else None,
        ppop_country_code=ref_data['sub_ppop'].country.country_code,
        ppop_zip_code=(ref_data['sub_ppop'].zip.zip5
                       if ref_data['sub_ppop'].zip else None),
    )

    # Contracts
    fpds_sub = SubmissionFactory(submission_id=2)
    fpds_base = DetachedAwardProcurementFactory(
        submission_id=fpds_sub.submission_id,
        action_date='2025-03-15',
    )
    fpds_latest = DetachedAwardProcurementFactory(
        submission_id=fpds_sub.submission_id,
        unique_award_key=fpds_base.unique_award_key,
        action_date='2025-03-16',
        awardee_or_recipient_uei=ref_data['prime_recipient'].uei,
        legal_entity_country_code=ref_data['prime_le'].country.country_code,
        legal_entity_zip4=ref_data['prime_le'].zip.zip5 if ref_data['prime_le'].zip else None,
        legal_entity_county_code=(ref_data['prime_le'].county.county_number
                                  if ref_data['prime_le'].county else None),
        place_of_perform_country_c=ref_data['prime_ppop'].country.country_code,
        place_of_performance_zip4a=(ref_data['prime_ppop'].zip.zip5
                                    if ref_data['prime_ppop'].zip else None),
        place_of_perform_county_co=(ref_data['prime_ppop'].county.county_number
                                    if ref_data['prime_ppop'].county else None),
    )
    sub_contract = SAMSubcontractFactory(
        unique_award_key=fpds_base.unique_award_key,
        uei=ref_data['sub_recipient'].uei,
        parent_uei=ref_data['sub_parent_recipient'].uei,
        date_submitted=datetime(2025, 3, 17, 16, 25, 12, 34),
        legal_entity_country_code=ref_data['sub_le'].country.country_code,
        legal_entity_zip_code=ref_data['sub_le'].zip.zip5 if ref_data['sub_le'].zip else None,
        ppop_country_code=ref_data['sub_ppop'].country.country_code,
        ppop_zip_code=(ref_data['sub_ppop'].zip.zip5
                       if ref_data['sub_ppop'].zip else None),
        action_date=datetime.now()
    )

    # Note: not including d1/fabs data (simulate getting the subaward data first)
    sess.add_all([sub_contract, sub_grant])
    sess.commit()

    # Grouping the values needed for tests
    fabs_grouped = {}
    all_fabs = [fabs_base, fabs_latest]
    for fabs in all_fabs:
        if fabs.unique_award_key not in fabs_grouped.keys():
            fabs_grouped[fabs.unique_award_key] = {'award_amount': fabs.federal_action_obligation,
                                                   'assistance_listing_num': fabs.assistance_listing_number,
                                                   'assistance_listing_title':
                                                       ref_data['assistance_listing_1'].program_title}
        else:
            fabs_grouped[fabs.unique_award_key]['award_amount'] += fabs.federal_action_obligation
            fabs_grouped[fabs.unique_award_key]['assistance_listing_num'] += ', ' + fabs.assistance_listing_number
            fabs_grouped[fabs.unique_award_key]['assistance_listing_title'] += \
                ', ' + ref_data['assistance_listing_2'].program_title

    populate_subaward_table(sess, 'contract', report_nums=[sub_contract.subaward_report_number])
    populate_subaward_table(sess, 'assistance', report_nums=[sub_grant.subaward_report_number])

    contracts_results = sess.query(Subaward).order_by(Subaward.id).\
        filter(Subaward.subaward_type == 'sub-contract').all()
    grants_results = sess.query(Subaward).order_by(Subaward.id).\
        filter(Subaward.subaward_type == 'sub-grant').all()
    original_ids = [result.id for result in contracts_results + grants_results]

    grant_created_at = grant_updated_at = grants_results[0].created_at
    contract_created_at = contract_updated_at = contracts_results[0].created_at
    drop_ref_fields = ['dom_country', 'dom_zip5', 'dom_zip9', 'dom_county_zip5', 'dom_county_zip9', 'int_country',
                       'prime_ppop', 'prime_le', 'sub_ppop', 'sub_le', 'assistance_listing_1', 'assistance_listing_2']
    for drop_ref_field in drop_ref_fields:
        ref_data.pop(drop_ref_field)

    # Expected Results - should be False as the award isn't provided
    assert compare_grant_results(grants_results[0], fabs_base, fabs_latest, fabs_grouped[fabs_latest.unique_award_key],
                                 sub_grant, grant_created_at, grant_updated_at, **ref_data) is False
    assert compare_contract_results(contracts_results[0], fpds_base, fpds_latest, sub_contract, contract_created_at,
                                    contract_updated_at, **ref_data) is False

    # now add the awards and fix the broken links
    sess.add_all([fabs_sub, fpds_sub, fabs_base, fabs_latest, fpds_base, fpds_latest])
    sess.commit()

    updated_proc_count = fix_broken_links(sess, 'contract')
    updated_grant_count = fix_broken_links(sess, 'assistance')

    assert updated_proc_count == 1
    assert updated_grant_count == 1

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
    assert compare_grant_results(grants_results[0], fabs_base, fabs_latest, fabs_grouped[fabs_latest.unique_award_key],
                                 sub_grant, grant_created_at, grant_updated_at, **ref_data) is True
    assert compare_contract_results(contracts_results[0], fpds_base, fpds_latest, sub_contract, contract_created_at,
                                    contract_updated_at, **ref_data) is True

    # Ensuring only updates occurred, no deletes/inserts
    assert set(original_ids) == set(updated_ids)
