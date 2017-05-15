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
    main_account_code = fuzzy.FuzzyText()
    sub_account_code = fuzzy.FuzzyText()
    internal_start_date = fuzzy.FuzzyDate(date(2015, 1, 1), date(2015, 12, 31))
    internal_end_date = None


class ProgramActivityFactory(factory.Factory):
    class Meta:
        model = domainModels.ProgramActivity

    program_activity_id = None
    budget_year = fuzzy.FuzzyText()
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
    program_number = fuzzy.FuzzyText()
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
