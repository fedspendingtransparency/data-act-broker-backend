from dataactcore.models.stagingModels import Appropriation
from dataactcore.models.domainModels import SF133
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "a12_appropriations"
_TAS = "a12_appropriations_tas"


def test_column_headers(database):
    expected_subset = {
        "uniqueid_TAS",
        "row_number",
        "adjustments_to_unobligated_cpe",
        "expected_value_SUM of GTAS SF133 Lines 1010 through 1067",
        "difference",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Tests that SF 133 amount sum for lines 1010 through 1042 matches Appropriation adjustments_to_unobligated_cpe
    for the specified fiscal year and period
    """
    tas = "".join([_TAS, "_success"])
    sub = SubmissionFactory(submission_id=1, reporting_fiscal_year=2016, reporting_fiscal_period=1)

    sf_1 = SF133(
        line=1020,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    sf_2 = SF133(
        line=1030,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    sf_3 = SF133(
        line=1040,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
        bea_category="a"
    )
    sf_4 = SF133(
        line=1040,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
        bea_category="b"
    )
    # This line should be ignored because it's before 2021
    sf_5 = SF133(
        line=1060,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=10,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    ap = Appropriation(job_id=1, row_number=1, display_tas=tas, adjustments_to_unobligated_cpe=4)
    ap_2 = Appropriation(job_id=1, row_number=2, display_tas="tas_no_sf", adjustments_to_unobligated_cpe=0)

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, sf_3, sf_4, sf_5, ap, ap_2], submission=sub) == 0


def test_success_post_2020(database):
    """Tests that SF 133 amount sum for lines 1010 through 1067 matches Appropriation adjustments_to_unobligated_cpe
    for the specified fiscal year and period if the year is over 2020
    """
    tas = "".join([_TAS, "_success"])
    sub = SubmissionFactory(submission_id=2, reporting_fiscal_year=2021, reporting_fiscal_period=1)

    sf_1 = SF133(
        line=1020,
        display_tas=tas,
        period=1,
        fiscal_year=2021,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    sf_2 = SF133(
        line=1030,
        display_tas=tas,
        period=1,
        fiscal_year=2021,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    sf_3 = SF133(
        line=1040,
        display_tas=tas,
        period=1,
        fiscal_year=2021,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    sf_4 = SF133(
        line=1067,
        display_tas=tas,
        period=1,
        fiscal_year=2021,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    ap = Appropriation(job_id=1, row_number=1, display_tas=tas, adjustments_to_unobligated_cpe=4)
    ap_2 = Appropriation(job_id=1, row_number=2, display_tas="tas_no_sf", adjustments_to_unobligated_cpe=0)

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, sf_3, sf_4, ap, ap_2], submission=sub) == 0


def test_failure(database):
    """Tests that SF 133 amount sum for lines 1010 through 1042 does not match Appropriation
    adjustments_to_unobligated_cpe for the specified fiscal year and period
    """
    tas = "".join([_TAS, "_failure"])

    sf_1 = SF133(
        line=1020,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    sf_2 = SF133(
        line=1030,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    sf_3 = SF133(
        line=1040,
        display_tas=tas,
        period=1,
        fiscal_year=2016,
        amount=1,
        agency_identifier="sys",
        main_account_code="000",
        sub_account_code="000",
    )
    ap = Appropriation(job_id=1, row_number=1, display_tas=tas, adjustments_to_unobligated_cpe=1)
    ap_2 = Appropriation(job_id=1, row_number=1, display_tas="tas_no_sf", adjustments_to_unobligated_cpe=1)

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, sf_3, ap, ap_2]) == 2
