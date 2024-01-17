from datetime import date, datetime, timezone

import factory
from factory import fuzzy

from dataactcore.models import fsrs


class _FSRSAttributes(factory.Factory):
    duns = fuzzy.FuzzyText()
    uei_number = fuzzy.FuzzyText()
    dba_name = fuzzy.FuzzyText()
    principle_place_city = fuzzy.FuzzyText()
    principle_place_street = None
    principle_place_state = fuzzy.FuzzyText()
    principle_place_state_name = fuzzy.FuzzyText()
    principle_place_country = fuzzy.FuzzyText()
    principle_place_zip = fuzzy.FuzzyText()
    principle_place_district = None
    parent_duns = fuzzy.FuzzyText()
    funding_agency_id = fuzzy.FuzzyText()
    funding_agency_name = fuzzy.FuzzyText()
    top_paid_fullname_1 = None
    top_paid_amount_1 = None
    top_paid_fullname_2 = None
    top_paid_amount_2 = None
    top_paid_fullname_3 = None
    top_paid_amount_3 = None
    top_paid_fullname_4 = None
    top_paid_amount_4 = None
    top_paid_fullname_5 = None
    top_paid_amount_5 = None


class _ContractAttributes(_FSRSAttributes):
    company_name = fuzzy.FuzzyText()
    bus_types = fuzzy.FuzzyText()
    company_address_city = fuzzy.FuzzyText()
    company_address_street = None
    company_address_state = fuzzy.FuzzyText()
    company_address_state_name = fuzzy.FuzzyText()
    company_address_country = fuzzy.FuzzyText()
    company_address_zip = fuzzy.FuzzyText()
    company_address_district = None
    parent_company_name = fuzzy.FuzzyText()
    naics = fuzzy.FuzzyText()
    funding_office_id = fuzzy.FuzzyText()
    funding_office_name = fuzzy.FuzzyText()
    recovery_model_q1 = fuzzy.FuzzyChoice((False, True))
    recovery_model_q2 = fuzzy.FuzzyChoice((False, True))


class _GrantAttributes(_FSRSAttributes):
    dunsplus4 = None
    awardee_name = fuzzy.FuzzyText()
    awardee_address_city = fuzzy.FuzzyText()
    awardee_address_street = None
    awardee_address_state = fuzzy.FuzzyText()
    awardee_address_state_name = fuzzy.FuzzyText()
    awardee_address_country = fuzzy.FuzzyText()
    awardee_address_zip = fuzzy.FuzzyText()
    awardee_address_district = None
    assistance_listing_numbers = fuzzy.FuzzyText()
    project_description = fuzzy.FuzzyText()
    compensation_q1 = fuzzy.FuzzyChoice((False, True))
    compensation_q2 = fuzzy.FuzzyChoice((False, True))
    federal_agency_id = fuzzy.FuzzyText()
    federal_agency_name = fuzzy.FuzzyText()


class _PrimeAwardAttributes(factory.Factory):
    internal_id = fuzzy.FuzzyText()
    date_submitted = fuzzy.FuzzyDateTime(datetime(2010, 1, 1, tzinfo=timezone.utc))
    report_period_mon = fuzzy.FuzzyText()
    report_period_year = fuzzy.FuzzyText()


class FSRSProcurementFactory(_ContractAttributes, _PrimeAwardAttributes):
    class Meta:
        model = fsrs.FSRSProcurement

    id = fuzzy.FuzzyInteger(999999)
    contract_number = fuzzy.FuzzyText()
    idv_reference_number = None
    report_type = fuzzy.FuzzyText()
    contract_agency_code = fuzzy.FuzzyText()
    contract_idv_agency_code = None
    contracting_office_aid = fuzzy.FuzzyText()
    contracting_office_aname = fuzzy.FuzzyText()
    contracting_office_id = fuzzy.FuzzyText()
    contracting_office_name = fuzzy.FuzzyText()
    treasury_symbol = fuzzy.FuzzyText()
    dollar_obligated = fuzzy.FuzzyText()
    date_signed = fuzzy.FuzzyDate(date(2010, 1, 1))
    transaction_type = fuzzy.FuzzyText()
    program_title = fuzzy.FuzzyText()
    subawards = []


class FSRSSubcontractFactory(_ContractAttributes):
    class Meta:
        model = fsrs.FSRSSubcontract

    id = fuzzy.FuzzyInteger(999999)
    parent_id = fuzzy.FuzzyInteger(999999)
    subcontract_amount = fuzzy.FuzzyText()
    subcontract_date = fuzzy.FuzzyDate(date(2010, 1, 1))
    subcontract_num = fuzzy.FuzzyText()
    overall_description = fuzzy.FuzzyText()
    recovery_subcontract_amt = None


class FSRSGrantFactory(_GrantAttributes, _PrimeAwardAttributes):
    class Meta:
        model = fsrs.FSRSGrant

    id = fuzzy.FuzzyInteger(999999)
    fain = fuzzy.FuzzyText()
    total_fed_funding_amount = fuzzy.FuzzyText()
    obligation_date = fuzzy.FuzzyDate(date(2010, 1, 1))


class FSRSSubgrantFactory(_GrantAttributes):
    class Meta:
        model = fsrs.FSRSSubgrant

    id = fuzzy.FuzzyInteger(999999)
    parent_id = fuzzy.FuzzyInteger(999999)
    subaward_amount = fuzzy.FuzzyText()
    subaward_date = fuzzy.FuzzyDate(date(2010, 1, 1))
    subaward_num = fuzzy.FuzzyText()


class SubawardFactory(factory.Factory):
    class Meta:
        model = fsrs.Subaward

    unique_award_key = fuzzy.FuzzyText()
    award_id = fuzzy.FuzzyText()
    parent_award_id = fuzzy.FuzzyText()
    award_amount = fuzzy.FuzzyText()
    action_date = fuzzy.FuzzyDate(date(2010, 1, 1))
    fy = fuzzy.FuzzyText()
    awarding_agency_code = fuzzy.FuzzyText()
    awarding_agency_name = fuzzy.FuzzyText()
    awarding_sub_tier_agency_c = fuzzy.FuzzyText()
    awarding_sub_tier_agency_n = fuzzy.FuzzyText()
    awarding_office_code = fuzzy.FuzzyText()
    awarding_office_name = fuzzy.FuzzyText()
    funding_agency_code = fuzzy.FuzzyText()
    funding_agency_name = fuzzy.FuzzyText()
    funding_sub_tier_agency_co = fuzzy.FuzzyText()
    funding_sub_tier_agency_na = fuzzy.FuzzyText()
    funding_office_code = fuzzy.FuzzyText()
    funding_office_name = fuzzy.FuzzyText()
    awardee_or_recipient_uei = fuzzy.FuzzyText()
    awardee_or_recipient_uniqu = fuzzy.FuzzyText()
    awardee_or_recipient_legal = fuzzy.FuzzyText()
    dba_name = fuzzy.FuzzyText()
    ultimate_parent_uei = fuzzy.FuzzyText()
    ultimate_parent_unique_ide = fuzzy.FuzzyText()
    ultimate_parent_legal_enti = fuzzy.FuzzyText()
    legal_entity_country_code = fuzzy.FuzzyText()
    legal_entity_country_name = fuzzy.FuzzyText()
    legal_entity_address_line1 = fuzzy.FuzzyText()
    legal_entity_city_name = fuzzy.FuzzyText()
    legal_entity_state_code = fuzzy.FuzzyText()
    legal_entity_state_name = fuzzy.FuzzyText()
    legal_entity_zip = fuzzy.FuzzyText()
    legal_entity_county_code = fuzzy.FuzzyText()
    legal_entity_county_name = fuzzy.FuzzyText()
    legal_entity_congressional = fuzzy.FuzzyText()
    legal_entity_foreign_posta = fuzzy.FuzzyText()
    business_types = fuzzy.FuzzyText()
    place_of_perform_city_name = fuzzy.FuzzyText()
    place_of_perform_state_code = fuzzy.FuzzyText()
    place_of_perform_state_name = fuzzy.FuzzyText()
    place_of_performance_zip = fuzzy.FuzzyText()
    place_of_performance_county_code = fuzzy.FuzzyText()
    place_of_performance_county_name = fuzzy.FuzzyText()
    place_of_perform_congressio = fuzzy.FuzzyText()
    place_of_perform_country_co = fuzzy.FuzzyText()
    place_of_perform_country_na = fuzzy.FuzzyText()
    award_description = fuzzy.FuzzyText()
    naics = fuzzy.FuzzyText()
    naics_description = fuzzy.FuzzyText()
    assistance_listing_numbers = fuzzy.FuzzyText()
    assistance_listing_titles = fuzzy.FuzzyText()

    subaward_type = fuzzy.FuzzyText()
    subaward_report_year = fuzzy.FuzzyText()
    subaward_report_month = fuzzy.FuzzyText()
    subaward_number = fuzzy.FuzzyText()
    subaward_amount = fuzzy.FuzzyText()
    sub_action_date = fuzzy.FuzzyDate(date(2010, 1, 1))
    sub_awardee_or_recipient_uei = fuzzy.FuzzyText()
    sub_awardee_or_recipient_uniqu = fuzzy.FuzzyText()
    sub_awardee_or_recipient_legal = fuzzy.FuzzyText()
    sub_dba_name = fuzzy.FuzzyText()
    sub_ultimate_parent_uei = fuzzy.FuzzyText()
    sub_ultimate_parent_unique_ide = fuzzy.FuzzyText()
    sub_ultimate_parent_legal_enti = fuzzy.FuzzyText()
    sub_legal_entity_country_code = fuzzy.FuzzyText()
    sub_legal_entity_country_name = fuzzy.FuzzyText()
    sub_legal_entity_address_line1 = fuzzy.FuzzyText()
    sub_legal_entity_city_name = fuzzy.FuzzyText()
    sub_legal_entity_state_code = fuzzy.FuzzyText()
    sub_legal_entity_state_name = fuzzy.FuzzyText()
    sub_legal_entity_zip = fuzzy.FuzzyText()
    sub_legal_entity_county_code = fuzzy.FuzzyText()
    sub_legal_entity_county_name = fuzzy.FuzzyText()
    sub_legal_entity_congressional = fuzzy.FuzzyText()
    sub_legal_entity_foreign_posta = fuzzy.FuzzyText()
    sub_business_types = fuzzy.FuzzyText()
    sub_place_of_perform_city_name = fuzzy.FuzzyText()
    sub_place_of_perform_state_code = fuzzy.FuzzyText()
    sub_place_of_perform_state_name = fuzzy.FuzzyText()
    sub_place_of_performance_zip = fuzzy.FuzzyText()
    sub_place_of_performance_county_code = fuzzy.FuzzyText()
    sub_place_of_performance_county_name = fuzzy.FuzzyText()
    sub_place_of_perform_congressio = fuzzy.FuzzyText()
    sub_place_of_perform_country_co = fuzzy.FuzzyText()
    sub_place_of_perform_country_na = fuzzy.FuzzyText()
    subaward_description = fuzzy.FuzzyText()
    sub_high_comp_officer1_full_na = fuzzy.FuzzyText()
    sub_high_comp_officer1_amount = fuzzy.FuzzyText()
    sub_high_comp_officer2_full_na = fuzzy.FuzzyText()
    sub_high_comp_officer2_amount = fuzzy.FuzzyText()
    sub_high_comp_officer3_full_na = fuzzy.FuzzyText()
    sub_high_comp_officer3_amount = fuzzy.FuzzyText()
    sub_high_comp_officer4_full_na = fuzzy.FuzzyText()
    sub_high_comp_officer4_amount = fuzzy.FuzzyText()
    sub_high_comp_officer5_full_na = fuzzy.FuzzyText()
    sub_high_comp_officer5_amount = fuzzy.FuzzyText()

    prime_id = fuzzy.FuzzyInteger(0, 100)
    internal_id = fuzzy.FuzzyText()
    date_submitted = fuzzy.FuzzyDateTime(datetime(2010, 1, 1, tzinfo=timezone.utc))
    report_type = fuzzy.FuzzyText()
    transaction_type = fuzzy.FuzzyText()
    program_title = fuzzy.FuzzyText()
    contract_agency_code = fuzzy.FuzzyText()
    contract_idv_agency_code = fuzzy.FuzzyText()
    grant_funding_agency_id = fuzzy.FuzzyText()
    grant_funding_agency_name = fuzzy.FuzzyText()
    federal_agency_name = fuzzy.FuzzyText()
    treasury_symbol = fuzzy.FuzzyText()
    dunsplus4 = fuzzy.FuzzyText()
    recovery_model_q1 = fuzzy.FuzzyText()
    recovery_model_q2 = fuzzy.FuzzyText()
    compensation_q1 = fuzzy.FuzzyText()
    compensation_q2 = fuzzy.FuzzyText()
    high_comp_officer1_full_na = fuzzy.FuzzyText()
    high_comp_officer1_amount = fuzzy.FuzzyText()
    high_comp_officer2_full_na = fuzzy.FuzzyText()
    high_comp_officer2_amount = fuzzy.FuzzyText()
    high_comp_officer3_full_na = fuzzy.FuzzyText()
    high_comp_officer3_amount = fuzzy.FuzzyText()
    high_comp_officer4_full_na = fuzzy.FuzzyText()
    high_comp_officer4_amount = fuzzy.FuzzyText()
    high_comp_officer5_full_na = fuzzy.FuzzyText()
    high_comp_officer5_amount = fuzzy.FuzzyText()

    sub_id = fuzzy.FuzzyInteger(0, 100)
    sub_parent_id = fuzzy.FuzzyInteger(0, 100)
    sub_federal_agency_id = fuzzy.FuzzyText()
    sub_federal_agency_name = fuzzy.FuzzyText()
    sub_funding_agency_id = fuzzy.FuzzyText()
    sub_funding_agency_name = fuzzy.FuzzyText()
    sub_funding_office_id = fuzzy.FuzzyText()
    sub_funding_office_name = fuzzy.FuzzyText()
    sub_naics = fuzzy.FuzzyText()
    sub_assistance_listing_numbers = fuzzy.FuzzyText()
    sub_dunsplus4 = fuzzy.FuzzyText()
    sub_recovery_subcontract_amt = fuzzy.FuzzyText()
    sub_recovery_model_q1 = fuzzy.FuzzyText()
    sub_recovery_model_q2 = fuzzy.FuzzyText()
    sub_compensation_q1 = fuzzy.FuzzyText()
    sub_compensation_q2 = fuzzy.FuzzyText()
