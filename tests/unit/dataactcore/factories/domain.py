from datetime import date

import factory
from factory import fuzzy

from dataactcore.models import domainModels


class SF133Factory(factory.Factory):
    class Meta:
        model = domainModels.SF133

    sf133_id = None
    agency_identifier = fuzzy.FuzzyText()
    allocation_transfer_agency = fuzzy.FuzzyText()
    availability_type_code = fuzzy.FuzzyText()
    beginning_period_of_availa = fuzzy.FuzzyText()
    ending_period_of_availabil = fuzzy.FuzzyText()
    main_account_code = fuzzy.FuzzyText()
    sub_account_code = fuzzy.FuzzyText()
    tas = fuzzy.FuzzyText()
    disaster_emergency_fund_code = fuzzy.FuzzyText()
    fiscal_year = fuzzy.FuzzyInteger(2010, 2040)
    period = fuzzy.FuzzyInteger(1, 12)
    line = fuzzy.FuzzyInteger(1, 9999)
    amount = 0


class CGACFactory(factory.Factory):
    class Meta:
        model = domainModels.CGAC

    cgac_id = None
    cgac_code = fuzzy.FuzzyText()
    agency_name = fuzzy.FuzzyText()


class FRECFactory(factory.Factory):
    class Meta:
        model = domainModels.FREC

    frec_id = None
    frec_code = fuzzy.FuzzyText()
    agency_name = fuzzy.FuzzyText()
    cgac = factory.SubFactory(CGACFactory)


class TASFactory(factory.Factory):
    class Meta:
        model = domainModels.TASLookup

    tas_id = None
    account_num = fuzzy.FuzzyInteger(1, 9999)
    allocation_transfer_agency = fuzzy.FuzzyText()
    agency_identifier = fuzzy.FuzzyText()
    beginning_period_of_availa = fuzzy.FuzzyText()
    ending_period_of_availabil = fuzzy.FuzzyText()
    availability_type_code = fuzzy.FuzzyText()
    fr_entity_description = fuzzy.FuzzyText()
    fr_entity_type = fuzzy.FuzzyText()
    main_account_code = fuzzy.FuzzyText()
    sub_account_code = fuzzy.FuzzyText()
    internal_start_date = fuzzy.FuzzyDate(date(2015, 1, 1), date(2015, 12, 31))
    internal_end_date = None


class ProgramActivityFactory(factory.Factory):
    class Meta:
        model = domainModels.ProgramActivity

    program_activity_id = None
    fiscal_year_period = fuzzy.FuzzyText()
    agency_id = fuzzy.FuzzyText()
    allocation_transfer_id = fuzzy.FuzzyText()
    account_number = fuzzy.FuzzyText()
    program_activity_code = fuzzy.FuzzyText()
    program_activity_name = fuzzy.FuzzyText()


class ObjectClassFactory(factory.Factory):
    class Meta:
        model = domainModels.ObjectClass

    object_class_id = None
    object_class_code = fuzzy.FuzzyText()
    object_class_name = fuzzy.FuzzyText()


class CFDAProgramFactory(factory.Factory):
    class Meta:
        model = domainModels.CFDAProgram
    cfda_program_id = None
    program_number = fuzzy.FuzzyDecimal(0, 99, 3)
    program_title = fuzzy.FuzzyText()
    popular_name = fuzzy.FuzzyText()
    federal_agency = fuzzy.FuzzyText()
    authorization = fuzzy.FuzzyText()
    objectives = fuzzy.FuzzyText()
    types_of_assistance = fuzzy.FuzzyText()
    uses_and_use_restrictions = fuzzy.FuzzyText()
    applicant_eligibility = fuzzy.FuzzyText()
    beneficiary_eligibility = fuzzy.FuzzyText()
    credentials_documentation = fuzzy.FuzzyText()
    preapplication_coordination = fuzzy.FuzzyText()
    application_procedures = fuzzy.FuzzyText()
    award_procedure = fuzzy.FuzzyText()
    deadlines = fuzzy.FuzzyText()
    range_of_approval_disapproval_time = fuzzy.FuzzyText()
    website_address = fuzzy.FuzzyText()
    formula_and_matching_requirements = fuzzy.FuzzyText()
    length_and_time_phasing_of_assistance = fuzzy.FuzzyText()
    reports = fuzzy.FuzzyText()
    audits = fuzzy.FuzzyText()
    records = fuzzy.FuzzyText()
    account_identification = fuzzy.FuzzyText()
    obligations = fuzzy.FuzzyText()
    range_and_average_of_financial_assistance = fuzzy.FuzzyText()
    appeals = fuzzy.FuzzyText()
    renewals = fuzzy.FuzzyText()
    program_accomplishments = fuzzy.FuzzyText()
    regulations_guidelines_and_literature = fuzzy.FuzzyText()
    regional_or_local_office = fuzzy.FuzzyText()
    headquarters_office = fuzzy.FuzzyText()
    related_programs = fuzzy.FuzzyText()
    examples_of_funded_projects = fuzzy.FuzzyText()
    criteria_for_selecting_proposals = fuzzy.FuzzyText()
    url = fuzzy.FuzzyText()
    recovery = fuzzy.FuzzyText()
    omb_agency_code = fuzzy.FuzzyText()
    omb_bureau_code = fuzzy.FuzzyText()
    published_date = fuzzy.FuzzyText()
    archived_date = fuzzy.FuzzyText()


class ZipsFactory(factory.Factory):
    class Meta:
        model = domainModels.Zips

    zips_id = None
    zip5 = fuzzy.FuzzyText()
    zip_last4 = fuzzy.FuzzyText()
    state_abbreviation = fuzzy.FuzzyText()
    county_number = fuzzy.FuzzyText()
    congressional_district_no = fuzzy.FuzzyText()


class ZipsGroupedFactory(factory.Factory):
    class Meta:
        model = domainModels.ZipsGrouped

    zips_grouped_id = None
    zip5 = fuzzy.FuzzyText()
    state_abbreviation = fuzzy.FuzzyText()
    county_number = fuzzy.FuzzyText()
    congressional_district_no = fuzzy.FuzzyText()


class SubTierAgencyFactory(factory.Factory):
    class Meta:
        model = domainModels.SubTierAgency

    sub_tier_agency_id = None
    sub_tier_agency_code = fuzzy.FuzzyText()
    sub_tier_agency_name = fuzzy.FuzzyText()
    cgac = factory.SubFactory(CGACFactory)
    frec = factory.SubFactory(FRECFactory)
    priority = fuzzy.FuzzyInteger(1, 2)
    is_frec = False


class OfficeFactory(factory.Factory):
    class Meta:
        model = domainModels.Office

    office_id = None
    office_code = fuzzy.FuzzyText()
    office_name = fuzzy.FuzzyText()
    sub_tier_code = fuzzy.FuzzyText()
    agency_code = fuzzy.FuzzyText()
    contract_awards_office = fuzzy.FuzzyChoice((False, True))
    contract_funding_office = fuzzy.FuzzyChoice((False, True))
    financial_assistance_awards_office = fuzzy.FuzzyChoice((False, True))
    financial_assistance_funding_office = fuzzy.FuzzyChoice((False, True))


class StatesFactory(factory.Factory):
    class Meta:
        model = domainModels.States

    states_id = None
    state_code = fuzzy.FuzzyText()
    state_name = fuzzy.FuzzyText()


class CountyCodeFactory(factory.Factory):
    class Meta:
        model = domainModels.CountyCode

    county_code_id = None
    county_number = fuzzy.FuzzyText()
    county_name = fuzzy.FuzzyText()
    state_code = fuzzy.FuzzyText()


class CityCodeFactory(factory.Factory):
    class Meta:
        model = domainModels.CityCode

    city_code_id = None
    feature_name = fuzzy.FuzzyText()
    feature_class = fuzzy.FuzzyText()
    city_code = fuzzy.FuzzyText()
    state_code = fuzzy.FuzzyText()
    county_number = fuzzy.FuzzyText()
    county_name = fuzzy.FuzzyText()
    latitude = fuzzy.FuzzyText()
    longitude = fuzzy.FuzzyText()


class ZipCityFactory(factory.Factory):
    class Meta:
        model = domainModels.ZipCity

    zip_city_id = None
    zip_code = fuzzy.FuzzyText()
    city_name = fuzzy.FuzzyText()


class CountryCodeFactory(factory.Factory):
    class Meta:
        model = domainModels.CountryCode

    country_code_id = None
    country_code = fuzzy.FuzzyText()
    country_name = fuzzy.FuzzyText()


class DunsFactory(factory.Factory):
    class Meta:
        model = domainModels.DUNS

    duns_id = None
    awardee_or_recipient_uniqu = fuzzy.FuzzyText()
    legal_business_name = fuzzy.FuzzyText()
    dba_name = fuzzy.FuzzyText()
    activation_date = fuzzy.FuzzyDate(date(2000, 1, 1), date(2020, 12, 31))
    deactivation_date = fuzzy.FuzzyDate(date(2000, 1, 1), date(2020, 12, 31))
    registration_date = fuzzy.FuzzyDate(date(2000, 1, 1), date(2020, 12, 31))
    expiration_date = fuzzy.FuzzyDate(date(2000, 1, 1), date(2020, 12, 31))
    last_sam_mod_date = fuzzy.FuzzyDate(date(2000, 1, 1), date(2020, 12, 31))
    address_line_1 = fuzzy.FuzzyText()
    address_line_2 = fuzzy.FuzzyText()
    city = fuzzy.FuzzyText()
    state = fuzzy.FuzzyText()
    zip = fuzzy.FuzzyText()
    zip4 = fuzzy.FuzzyText()
    country_code = fuzzy.FuzzyText()
    congressional_district = fuzzy.FuzzyText()
    entity_structure = fuzzy.FuzzyText()
    business_types_codes = ['A', 'B', 'C']
    business_types = ['Full Name A', 'Full Name B', 'Full Name C']
    ultimate_parent_unique_ide = fuzzy.FuzzyText()
    ultimate_parent_legal_enti = fuzzy.FuzzyText()
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
    last_exec_comp_mod_date = fuzzy.FuzzyDate(date(2000, 1, 1), date(2020, 12, 31))


class StateCongressionalFactory(factory.Factory):
    class Meta:
        model = domainModels.StateCongressional

    state_congressional_id = None
    state_code = fuzzy.FuzzyText()
    congressional_district_no = fuzzy.FuzzyText()
    census_year = fuzzy.FuzzyInteger(1990, 2040)
