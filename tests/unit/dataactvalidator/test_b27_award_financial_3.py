from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "b27_award_financial_3"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "program_activity_code",
        "program_activity_name",
        "uniqueid_TAS",
        "uniqueid_ObjectClass",
    }
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """As of FY26, each row in file C must not contain a PAC/PAN."""

    sub = SubmissionFactory(submission_id=1, reporting_fiscal_year=2026)
    # No PAC/PAN
    af1 = AwardFinancialFactory(program_activity_code=None, program_activity_name=None)
    assert number_of_errors(_FILE, database, models=[af1], submission=sub) == 0

    # Ignored prior to FY 2026
    sub = SubmissionFactory(submission_id=2, reporting_fiscal_year=2025)
    # No PAC/PAN
    af1 = AwardFinancialFactory(program_activity_code="", program_activity_name="")
    af2 = AwardFinancialFactory(program_activity_code=None, program_activity_name=None)
    # Has a PAC/PAN
    af3 = AwardFinancialFactory(program_activity_code="342", program_activity_name="Test")
    assert number_of_errors(_FILE, database, models=[af1, af2, af3], submission=sub) == 0


def test_failure(database):
    """Test failure as of FY26, each row in file C must contain a PARK and must not contain a PAC/PAN."""

    sub = SubmissionFactory(submission_id=3, reporting_fiscal_year=2026)
    # Has both PAC/PAN
    af1 = AwardFinancialFactory(program_activity_code="1234", program_activity_name="1234")
    # Has a PAC
    af2 = AwardFinancialFactory(program_activity_code="1234", program_activity_name=None)
    # Has a PAN
    af3 = AwardFinancialFactory(program_activity_code=None, program_activity_name="Test")

    assert number_of_errors(_FILE, database, models=[af1, af2, af3], submission=sub) == 3
