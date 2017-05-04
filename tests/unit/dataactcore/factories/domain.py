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


class ZipsFactory(factory.Factory):
    class Meta:
        model = domainModels.Zips

    zips_id = None
    zip5 = fuzzy.FuzzyText()
    zip_last4 = fuzzy.FuzzyText()
    state_abbreviation = fuzzy.FuzzyText()
    county_number = fuzzy.FuzzyText()
    congressional_district_no = fuzzy.FuzzyText()
