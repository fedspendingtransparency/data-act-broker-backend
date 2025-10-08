from dataactcore.models.stagingModels import Appropriation
from dataactcore.models.domainModels import SF133
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "a8_appropriations"
_TAS = "a8_appropriations_tas"


def test_column_headers(database):
    expected_subset = {
        "uniqueid_TAS",
        "row_number",
        "budget_authority_appropria_cpe",
        "expected_value_SUM of GTAS SF133 Lines 1160, 1180, 1260, 1280",
        "difference",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Tests that SF 133 amount sum for lines 1160, 1180, 1260, 1280 matches Appropriation
    budget_authority_appropria_cpe for the specified fiscal year and period
    """

    tas = "".join([_TAS, "_success"])

    sf_1 = SF133(
        line=1160,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    sf_2 = SF133(
        line=1180,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    sf_3 = SF133(
        line=1260,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    sf_4 = SF133(
        line=1280,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    ap = Appropriation(job_id=1, row_number=1, display_tas=tas, budget_authority_appropria_cpe=4)

    models = [sf_1, sf_2, sf_3, sf_4, ap]

    assert number_of_errors(_FILE, database, models=models) == 0


def test_failure(database):
    """Tests that SF 133 amount sum for lines 1160, 1180, 1260, 1280 does not match Appropriation
    budget_authority_appropria_cpe for the specified fiscal year and period
    """
    tas = "".join([_TAS, "_failure"])

    sf_1 = SF133(
        line=1160,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    sf_2 = SF133(
        line=1180,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    sf_3 = SF133(
        line=1260,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    sf_4 = SF133(
        line=1280,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    ap = Appropriation(job_id=1, row_number=1, display_tas=tas, budget_authority_appropria_cpe=1)

    models = [sf_1, sf_2, sf_3, sf_4, ap]

    assert number_of_errors(_FILE, database, models=models) == 1
