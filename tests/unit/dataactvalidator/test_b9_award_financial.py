from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactcore.factories.domain import ProgramActivityFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b9_award_financial'


def test_column_headers(database):
    expected_subset = {'row_number', 'beginning_period_of_availa', 'agency_identifier', 'allocation_transfer_agency',
                       'main_account_code', 'program_activity_name', 'program_activity_code'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Testing valid program activity name for the corresponding TAS/TAFS as defined in Section 82 of OMB Circular
    A-11. """

    af_1 = AwardFinancialFactory(row_number=1, agency_identifier='test',
                                 allocation_transfer_agency='test', main_account_code='test',
                                 program_activity_name='test', program_activity_code='test')

    af_2 = AwardFinancialFactory(row_number=2, agency_identifier='test',
                                 allocation_transfer_agency='test', main_account_code='test',
                                 program_activity_name='test', program_activity_code='test')

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    assert number_of_errors(_FILE, database, models=[af_1, af_2, pa]) == 0


def test_success_null(database):
    """Program activity name/code as null"""
    af = AwardFinancialFactory(row_number=1, agency_identifier='test',
                               allocation_transfer_agency='test', main_account_code='test',
                               program_activity_name=None, program_activity_code=None)

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test')

    assert number_of_errors(_FILE, database, models=[af, pa]) == 0


def test_success_fiscal_year(database):
    """ Testing valid name for FY that matches with budget_year"""

    af = AwardFinancialFactory(row_number=1, submission_id='1', agency_identifier='test',
                                 allocation_transfer_agency='test', main_account_code='test',
                                 program_activity_name='test', program_activity_code='test')

    pa_1 = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    pa_2 = ProgramActivityFactory(budget_year=2017, agency_id='test2', allocation_transfer_id='test2',
                                account_number='test2', program_activity_name='test2', program_activity_code='test2')

    submission = SubmissionFactory(submission_id='1', reporting_fiscal_year='2016')

    assert number_of_errors(_FILE, database, models=[af, pa_1, pa_2], submission=submission) == 0


def test_failure_fiscal_year(database):
    """ Testing invalid name for FY, not matches with budget_year"""

    af = AwardFinancialFactory(row_number=1, submission_id='1', agency_identifier='test2',
                                 allocation_transfer_agency='test2', main_account_code='test2',
                                 program_activity_name='test2', program_activity_code='test2')

    pa_1 = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    pa_2 = ProgramActivityFactory(budget_year=2017, agency_id='test2', allocation_transfer_id='test2',
                                account_number='test2', program_activity_name='test2', program_activity_code='test2')

    submission = SubmissionFactory(submission_id='1', reporting_fiscal_year='2016')

    assert number_of_errors(_FILE, database, models=[af, pa_1, pa_2], submission=submission) == 1


def test_failure_program_activity_name(database):
    """ Testing invalid program activity name for the corresponding TAS/TAFS as defined in Section 82 of OMB Circular
    A-11. """

    af_1 = AwardFinancialFactory(row_number=1, agency_identifier='test',
                                 allocation_transfer_agency='test', main_account_code='test',
                                 program_activity_name='test_wrong', program_activity_code='test')

    af_2 = AwardFinancialFactory(row_number=1, agency_identifier='test',
                                 allocation_transfer_agency='test', main_account_code='test',
                                 program_activity_name='test_wrong', program_activity_code='0000')

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    assert number_of_errors(_FILE, database, models=[af_1, af_2, pa]) == 1


def test_failure_program_activity_code(database):
    """Failure where the program _activity_code does not match"""
    af_1 = AwardFinancialFactory(row_number=1, agency_identifier='test',
                                 allocation_transfer_agency='test', main_account_code='test',
                                 program_activity_name='test', program_activity_code='test_wrong')

    af_2 = AwardFinancialFactory(row_number=1, agency_identifier='test',
                                 allocation_transfer_agency='test', main_account_code='test',
                                 program_activity_name='Unknown/Other', program_activity_code='12345')

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    assert number_of_errors(_FILE, database, models=[af_1, af_2, pa]) == 1


def test_success_null_program_activity(database):
    """program activity name/code as null"""
    af = AwardFinancialFactory(row_number=1, agency_identifier='test_wrong',
                               allocation_transfer_agency='test', main_account_code='test',
                               program_activity_name=None, program_activity_code=None)

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test')

    assert number_of_errors(_FILE, database, models=[af, pa]) == 0
