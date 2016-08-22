from datetime import date, datetime, timezone

import factory
from factory import fuzzy

from dataactcore.models import fsrs


def field_str(name):
    return lambda idx: name + str(idx)


class _FSRSCommon:
    duns = fuzzy.FuzzyText()
    company_name = fuzzy.FuzzyText()
    dba_name = fuzzy.FuzzyText()
    bus_types = fuzzy.FuzzyText()
    company_address_city = fuzzy.FuzzyText()
    company_address_street = None
    company_address_state = fuzzy.FuzzyText()
    company_address_country = fuzzy.FuzzyText()
    company_address_zip = fuzzy.FuzzyText()
    company_address_district = None
    principle_place_city = fuzzy.FuzzyText()
    principle_place_street = None
    principle_place_state = fuzzy.FuzzyText()
    principle_place_country = fuzzy.FuzzyText()
    principle_place_zip = fuzzy.FuzzyText()
    principle_place_district = None
    parent_duns = fuzzy.FuzzyText()
    parent_company_name = fuzzy.FuzzyText()
    naics = fuzzy.FuzzyText()
    funding_agency_id = fuzzy.FuzzyText()
    funding_agency_name = fuzzy.FuzzyText()
    funding_office_id = fuzzy.FuzzyText()
    funding_office_name = fuzzy.FuzzyText()
    recovery_model_q1 = fuzzy.FuzzyChoice((False, True))
    recovery_model_q2 = fuzzy.FuzzyChoice((False, True))
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


class FSRSAwardFactory(factory.Factory, _FSRSCommon):
    class Meta:
        model = fsrs.FSRSAward

    id = factory.Sequence(int)
    internal_id = fuzzy.FuzzyText()
    contract_number = fuzzy.FuzzyText()
    idv_reference_number = None
    date_submitted = fuzzy.FuzzyDateTime(
        datetime(2010, 1, 1, tzinfo=timezone.utc))
    report_period_mon = fuzzy.FuzzyText()
    report_period_year = fuzzy.FuzzyText()
    report_type = fuzzy.FuzzyText()
    contract_agency_code = fuzzy.FuzzyText()
    contract_idv_agency_code = None
    contracting_office_aid = fuzzy.FuzzyText()
    contracting_office_aname = fuzzy.FuzzyText()
    contracting_office_id = fuzzy.FuzzyText()
    contracting_office_name = fuzzy.FuzzyText()
    treasury_symbol = fuzzy.FuzzyText()
    dollar_obligated = fuzzy.FuzzyText()
    date_signed = fuzzy.FuzzyDate(date(2011, 1, 1))
    transaction_type = fuzzy.FuzzyText()
    program_title = fuzzy.FuzzyText()
    subawards = []


class FSRSSubawardFactory(factory.Factory, _FSRSCommon):
    class Meta:
        model = fsrs.FSRSSubaward

    id = factory.Sequence(int)
    subcontract_amount = fuzzy.FuzzyText()
    subcontract_date = fuzzy.FuzzyDate(date(2012, 2, 2))
    subcontract_num = fuzzy.FuzzyText()
    overall_description = fuzzy.FuzzyText()
    recovery_subcontract_amt = None
