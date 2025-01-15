from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'b27_award_financial_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'program_activity_reporting_key', 'uniqueid_TAS', 'uniqueid_ObjectClass'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ FY26 forward, each row in file B must contain PARK. """

    # Has a PARK
    sub = SubmissionFactory(submission_id=1, reporting_fiscal_year=2026)
    af = AwardFinancialFactory(program_activity_reporting_key='aBcD')

    assert number_of_errors(_FILE, database, models=[af], submission=sub) == 0

    # Ignored for FY before 2025
    sub = SubmissionFactory(submission_id=2, reporting_fiscal_year=2025)
    af1 = AwardFinancialFactory(program_activity_reporting_key='')
    af2 = AwardFinancialFactory(program_activity_reporting_key=None)

    assert number_of_errors(_FILE, database, models=[af1, af2], submission=sub) == 0


def test_failure(database):
    """ Failure FY26 forward, each row in file B must contain PARK. """

    # No PARK
    sub = SubmissionFactory(submission_id=3, reporting_fiscal_year=2026)
    af1 = AwardFinancialFactory(program_activity_reporting_key='')
    af2 = AwardFinancialFactory(program_activity_reporting_key=None)

    assert number_of_errors(_FILE, database, models=[af1, af2], submission=sub) == 2
