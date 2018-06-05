from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactcore.factories.domain import ProgramActivityFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns, populate_publish_status
from dataactcore.models.lookups import PUBLISH_STATUS_DICT


_FILE = 'b9_award_financial'


def test_column_headers(database):
    expected_subset = {'row_number', 'agency_identifier', 'main_account_code',
                       'program_activity_name', 'program_activity_code'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Testing valid program activity name for the corresponding TAS/TAFS as defined in Section 82 of OMB Circular
         A-11.
    """

    populate_publish_status(database)

    af_1 = AwardFinancialFactory(row_number=1, agency_identifier='test', submission_id=1, main_account_code='test',
                                 program_activity_name='test', program_activity_code='test')

    af_2 = AwardFinancialFactory(row_number=2, agency_identifier='test', submission_id=1,  main_account_code='test',
                                 program_activity_name='test', program_activity_code='test')

    pa = ProgramActivityFactory(fiscal_year_quarter='FY2017Q1', agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    submission = SubmissionFactory(submission_id=1, reporting_fiscal_year='2017', reporting_fiscal_period=3,
                                   publish_status_id=PUBLISH_STATUS_DICT['unpublished'])

    assert number_of_errors(_FILE, database, models=[af_1, af_2, pa], submission=submission) == 0


def test_success_null(database):
    """Program activity name/code as null """

    populate_publish_status(database)

    af = AwardFinancialFactory(row_number=1, agency_identifier='test', main_account_code='test',
                               program_activity_name=None, program_activity_code=None)

    pa = ProgramActivityFactory(fiscal_year_quarter='FY2017Q2', agency_id='test', allocation_transfer_id='test',
                                account_number='test')

    assert number_of_errors(_FILE, database, models=[af, pa]) == 0


def test_success_fiscal_year_quarter(database):
    """ Testing valid name for program_activity that matches with fiscal_year_quarter """

    populate_publish_status(database)

    af = AwardFinancialFactory(row_number=1, submission_id=1, agency_identifier='test',
                               main_account_code='test', program_activity_name='test', program_activity_code='test')

    pa = ProgramActivityFactory(fiscal_year_quarter='FY2016Q1', agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    submission = SubmissionFactory(submission_id=1, reporting_fiscal_year='2016', reporting_fiscal_period=3,
                                   publish_status_id=PUBLISH_STATUS_DICT['unpublished'])

    assert number_of_errors(_FILE, database, models=[af, pa], submission=submission) == 0


def test_failure_fiscal_year_quarter(database):
    """ Testing invalid program_activity, does not match with fiscal_year_quarter """

    populate_publish_status(database)

    af = AwardFinancialFactory(row_number=1, submission_id=1, agency_identifier='test',
                               main_account_code='test', program_activity_name='test',
                               program_activity_code='test')

    pa_1 = ProgramActivityFactory(fiscal_year_quarter='FQY', agency_id='test', allocation_transfer_id='test',
                                  account_number='test', program_activity_name='test', program_activity_code='test')

    pa_2 = ProgramActivityFactory(fiscal_year_quarter='FY2016Q1', agency_id='test2', allocation_transfer_id='test2',
                                  account_number='test2', program_activity_name='test2', program_activity_code='test2')

    submission = SubmissionFactory(submission_id=1, reporting_fiscal_year='2016', reporting_fiscal_period=3,
                                   publish_status_id=PUBLISH_STATUS_DICT['unpublished'])

    assert number_of_errors(_FILE, database, models=[af, pa_1, pa_2], submission=submission) == 1


def test_success_ignore_recertification(database):
    """ Testing invalid program_activity, ignored since recertification for FY2017Q2 or FY2017Q3 """

    populate_publish_status(database)

    af = AwardFinancialFactory(row_number=1, submission_id=1, agency_identifier='test',
                               main_account_code='test', program_activity_name='test', program_activity_code='test')

    pa = ProgramActivityFactory(fiscal_year_quarter='FY2017Q3', agency_id='test2', allocation_transfer_id='test2',
                                account_number='test2', program_activity_name='test2', program_activity_code='test2')

    submission = SubmissionFactory(submission_id=1, reporting_fiscal_year='2017', reporting_fiscal_period=9,
                                   publish_status_id=PUBLISH_STATUS_DICT['published'])

    assert number_of_errors(_FILE, database, models=[af, pa], submission=submission) == 0


def test_failure_recertification(database):
    """ Testing invalid program_activity, not ignored since not recertification for FY2017Q2 or FY2017Q3 """

    populate_publish_status(database)

    af = AwardFinancialFactory(row_number=1, submission_id=1, agency_identifier='test',
                               main_account_code='test', program_activity_name='test', program_activity_code='test')

    pa = ProgramActivityFactory(fiscal_year_quarter='FY2017Q3', agency_id='test2', allocation_transfer_id='test2',
                                account_number='test2', program_activity_name='test2', program_activity_code='test2')

    submission = SubmissionFactory(submission_id=1, reporting_fiscal_year='2017', reporting_fiscal_period=9,
                                   publish_status_id=PUBLISH_STATUS_DICT['unpublished'])

    assert number_of_errors(_FILE, database, models=[af, pa], submission=submission) == 1


def test_success_ignore_case(database):
    """ Testing program activity validation to ignore case """

    populate_publish_status(database)

    af = AwardFinancialFactory(row_number=1, submission_id=1, agency_identifier='test', main_account_code='test',
                               program_activity_name='TEST', program_activity_code='test')

    pa = ProgramActivityFactory(fiscal_year_quarter='FY2016Q4', agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    submission = SubmissionFactory(submission_id=1, reporting_fiscal_year='2016', reporting_fiscal_period=12,
                                   publish_status_id=PUBLISH_STATUS_DICT['unpublished'])

    assert number_of_errors(_FILE, database, models=[af, pa], submission=submission) == 0


def test_failure_program_activity_name(database):
    """ Testing invalid program_activity_name for the corresponding TAS/TAFS as defined in Section 82 of OMB Circular
         A-11.
    """

    populate_publish_status(database)

    af_1 = AwardFinancialFactory(row_number=1, submission_id=1, agency_identifier='test',
                                 main_account_code='test', program_activity_name='test_wrong',
                                 program_activity_code='test')

    af_2 = AwardFinancialFactory(row_number=1, submission_id=1, agency_identifier='test',
                                 main_account_code='test', program_activity_name='test_wrong',
                                 program_activity_code='0000')

    pa = ProgramActivityFactory(fiscal_year_quarter='FY2015Q5', agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    submission = SubmissionFactory(submission_id=1, reporting_fiscal_year='2015', reporting_fiscal_period=15,
                                   publish_status_id=PUBLISH_STATUS_DICT['unpublished'])

    assert number_of_errors(_FILE, database, models=[af_1, af_2, pa], submission=submission) == 1


def test_failure_program_activity_code(database):
    """ Failure where the program_activity_code does not match """

    populate_publish_status(database)

    af_1 = AwardFinancialFactory(row_number=1, submission_id=1, agency_identifier='test',
                                 main_account_code='test', program_activity_name='test',
                                 program_activity_code='test_wrong')

    af_2 = AwardFinancialFactory(row_number=1, submission_id=1, agency_identifier='test', main_account_code='test',
                                 program_activity_name='Unknown/Other', program_activity_code='12345')

    pa = ProgramActivityFactory(fiscal_year_quarter='FY2016Q4', agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    submission = SubmissionFactory(submission_id=1, reporting_fiscal_year='2016', reporting_fiscal_period=12,
                                   publish_status_id=PUBLISH_STATUS_DICT['unpublished'])

    assert number_of_errors(_FILE, database, models=[af_1, af_2, pa], submission=submission) == 1


def test_success_null_program_activity(database):
    """ program activity name/code as null """
    af = AwardFinancialFactory(row_number=1, agency_identifier='test_wrong',
                               main_account_code='test', program_activity_name=None, program_activity_code=None)

    pa = ProgramActivityFactory(fiscal_year_quarter='FYQ', agency_id='test', allocation_transfer_id='test',
                                account_number='test')

    assert number_of_errors(_FILE, database, models=[af, pa]) == 0
