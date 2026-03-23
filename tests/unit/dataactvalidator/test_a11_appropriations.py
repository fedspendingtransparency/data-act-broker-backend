from dataactcore.models.stagingModels import Appropriation
from dataactcore.models.domainModels import SF133
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "a11_appropriations"
_TAS = "a11_appropriations_tas"


def test_column_headers(database):
    expected_subset = {
        "uniqueid_TAS",
        "row_number",
        "spending_authority_from_of_cpe",
        "expected_value_SUM of GTAS SF133 Lines 1750, 1850",
        "difference",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Tests that SF 133 amount sum for lines 1750, 1850 matches Appropriation spending_authority_from_of_cpe for the
    specified fiscal year and period
    """
    tas_1 = "".join([_TAS, "_success"])
    tas_2 = "".join([_TAS, "_success_2"])

    sf_1 = SF133(
        line=1750,
        display_tas=tas_1,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    sf_2 = SF133(
        line=1850,
        display_tas=tas_1,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
        bea_category="a",
    )
    sf_3 = SF133(
        line=1850,
        display_tas=tas_1,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
        bea_category="b",
    )
    sf_4 = SF133(
        line=1750,
        display_tas=tas_2,
        period=1,
        fiscal_year=2016,
        amount=0,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    sf_5 = SF133(
        line=1850,
        display_tas=tas_2,
        period=1,
        fiscal_year=2016,
        amount=0,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    ap_1 = Appropriation(job_id=1, row_number=1, display_tas=tas_1, spending_authority_from_of_cpe=3)
    ap_2 = Appropriation(job_id=2, row_number=1, display_tas=tas_2, spending_authority_from_of_cpe=None)
    ap_3 = Appropriation(job_id=2, row_number=1, display_tas="tas_no_sf", spending_authority_from_of_cpe=0)

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, sf_3, sf_4, sf_5, ap_1, ap_2, ap_3]) == 0


def test_failure(database):
    """Tests that SF 133 amount sum for lines 1750, 1850 does not match Appropriation spending_authority_from_of_cpe
    for the specified fiscal year and period
    """
    tas = "".join([_TAS, "_failure"])

    sf_1 = SF133(
        line=1750,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    sf_2 = SF133(
        line=1850,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    ap_1 = Appropriation(job_id=1, row_number=1, display_tas=tas, spending_authority_from_of_cpe=1)
    ap_2 = Appropriation(job_id=1, row_number=1, display_tas=tas, spending_authority_from_of_cpe=None)
    ap_3 = Appropriation(job_id=1, row_number=1, display_tas="tas_no_sf", spending_authority_from_of_cpe=1)

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, ap_1, ap_2, ap_3]) == 3
