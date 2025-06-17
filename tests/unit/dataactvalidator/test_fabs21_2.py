from datetime import date
from dateutil.relativedelta import relativedelta

from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactcore.factories.domain import OfficeFactory
from dataactcore.models.domainModels import SubTierAgency, CGAC, FREC
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs21_2"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "funding_sub_tier_agency_co",
        "funding_office_code",
        "action_date",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """If both are submitted, FundingSubTierAgencyCode and FundingOfficeCode must belong to the same FundingAgencyCode
    (per the Federal Hierarchy) at the time the award was signed (per the Action Date). Ignored if one or both are
    missing.
    """
    cgac = CGAC(cgac_id=1, cgac_code="001", agency_name="test")
    frec = FREC(frec_id=1, cgac_id=1, frec_code="0001", agency_name="test2")
    # sub tier codes are different on these offices to prove that we don't care if the office is under that sub tier
    # as long as the top tier codes match
    office_1 = OfficeFactory(
        office_code="12345a",
        sub_tier_code="abcd",
        agency_code=cgac.cgac_code,
        effective_start_date="01/01/2020",
        effective_end_date=None,
    )
    office_2 = OfficeFactory(
        office_code="123457",
        sub_tier_code="efgh",
        agency_code=frec.frec_code,
        effective_start_date="01/01/2020",
        effective_end_date=date.today() + relativedelta(days=1),
    )
    agency_1 = SubTierAgency(sub_tier_agency_code="a000", cgac_id=1, frec_id=1, is_frec=False)
    agency_2 = SubTierAgency(sub_tier_agency_code="0001", cgac_id=1, frec_id=1, is_frec=True)

    # Same agency for cgac
    fabs_1 = FABSFactory(
        funding_sub_tier_agency_co=agency_1.sub_tier_agency_code,
        funding_office_code=office_1.office_code,
        correction_delete_indicatr="",
        action_date=date.today(),
    )
    # Same agency for cgac (uppercase)
    fabs_2 = FABSFactory(
        funding_sub_tier_agency_co=agency_1.sub_tier_agency_code.upper(),
        funding_office_code=office_1.office_code.upper(),
        correction_delete_indicatr=None,
        action_date=date.today(),
    )
    # Same agency for frec
    fabs_3 = FABSFactory(
        funding_sub_tier_agency_co=agency_2.sub_tier_agency_code,
        funding_office_code=office_2.office_code,
        correction_delete_indicatr="c",
        action_date=date.today(),
    )
    # Missing sub tier code
    fabs_4 = FABSFactory(
        funding_sub_tier_agency_co="",
        funding_office_code=office_2.office_code,
        correction_delete_indicatr="C",
        action_date=date.today(),
    )
    # Missing office code
    fabs_5 = FABSFactory(
        funding_sub_tier_agency_co=agency_1.sub_tier_agency_code,
        funding_office_code=None,
        correction_delete_indicatr="",
        action_date=date.today(),
    )
    # Ignore correction delete indicator of D
    fabs_6 = FABSFactory(
        funding_sub_tier_agency_co=agency_1.sub_tier_agency_code,
        funding_office_code=office_2.office_code,
        correction_delete_indicatr="d",
        action_date=date.today(),
    )

    errors = number_of_errors(
        _FILE,
        database,
        models=[cgac, frec, office_1, office_2, agency_1, agency_2, fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6],
    )
    assert errors == 0


def test_failure_different_agencies(database):
    """Test failure if both are submitted, FundingSubTierAgencyCode and FundingOfficeCode must belong to the same
    FundingAgencyCode (per the Federal Hierarchy).
    """
    cgac = CGAC(cgac_id=1, cgac_code="001", agency_name="test")
    frec = FREC(frec_id=1, cgac_id=1, frec_code="0001", agency_name="test2")
    office = OfficeFactory(
        office_code="123456",
        sub_tier_code="abcd",
        agency_code=cgac.cgac_code,
        effective_start_date=date.today(),
        effective_end_date=None,
    )
    agency = SubTierAgency(sub_tier_agency_code="0000", frec_id=1, cgac_id=1, is_frec=True)

    # Sub tier is FREC, office is based on CGAC, the numbers are different
    fabs = FABSFactory(
        funding_sub_tier_agency_co=agency.sub_tier_agency_code,
        funding_office_code=office.office_code,
        correction_delete_indicatr="",
        action_date=date.today(),
    )

    errors = number_of_errors(_FILE, database, models=[fabs, cgac, frec, office, agency])
    assert errors == 1


def test_failure_inactive_office(database):
    """Test failure if office is inactive on the action date"""
    cgac = CGAC(cgac_id=1, cgac_code="001", agency_name="test")
    frec = FREC(frec_id=1, cgac_id=1, frec_code="0001", agency_name="test")
    office = OfficeFactory(
        office_code="123456",
        sub_tier_code="abcd",
        agency_code=cgac.cgac_code,
        effective_start_date=date.today(),
        effective_end_date=date.today() + relativedelta(days=5),
    )
    agency = SubTierAgency(sub_tier_agency_code="0000", frec_id=1, cgac_id=1, is_frec=False)

    # Action date is a day before the start date
    fabs_1 = FABSFactory(
        funding_sub_tier_agency_co=agency.sub_tier_agency_code,
        funding_office_code=office.office_code,
        correction_delete_indicatr="",
        action_date=date.today() - relativedelta(days=1),
    )
    # Action date is after the end date
    fabs_2 = FABSFactory(
        funding_sub_tier_agency_co=agency.sub_tier_agency_code,
        funding_office_code=office.office_code,
        correction_delete_indicatr="",
        action_date=date.today() + relativedelta(months=1),
    )

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, cgac, frec, office, agency])
    assert errors == 2
