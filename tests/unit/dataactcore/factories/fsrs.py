from datetime import date, datetime, timezone

import factory
from factory import fuzzy

from dataactcore.models import fsrs


class _FSRSAttributes(factory.Factory):
    duns = fuzzy.FuzzyText()
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
    cfda_numbers = fuzzy.FuzzyText()
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

    subcontract_amount = fuzzy.FuzzyText()
    subcontract_date = fuzzy.FuzzyDate(date(2010, 1, 1))
    subcontract_num = fuzzy.FuzzyText()
    overall_description = fuzzy.FuzzyText()
    recovery_subcontract_amt = None


class FSRSGrantFactory(_GrantAttributes, _PrimeAwardAttributes):
    class Meta:
        model = fsrs.FSRSGrant

    fain = fuzzy.FuzzyText()
    total_fed_funding_amount = fuzzy.FuzzyText()
    obligation_date = fuzzy.FuzzyDate(date(2010, 1, 1))


class FSRSSubgrantFactory(_GrantAttributes):
    class Meta:
        model = fsrs.FSRSSubgrant

    subaward_amount = fuzzy.FuzzyText()
    subaward_date = fuzzy.FuzzyDate(date(2010, 1, 1))
    subaward_num = fuzzy.FuzzyText()
