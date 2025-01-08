from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'b27_object_class_program_activity_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'program_activity_reporting_key', 'program_activity_code', 'program_activity_name',
                       'uniqueid_TAS', 'uniqueid_ObjectClass'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Pre-FY26, each row in file C must contain either PAC/PAN or PARK. """

    sub = SubmissionFactory(submission_id=1, reporting_fiscal_year=2025)
    # Has a PARK and a PAC/PAN
    op1 = ObjectClassProgramActivityFactory(program_activity_reporting_key='aBcD', program_activity_code='342',
                                            program_activity_name='Test')
    # Has a PARK but not PAC/PAN
    op2 = ObjectClassProgramActivityFactory(program_activity_reporting_key='aBcD', program_activity_code='',
                                            program_activity_name='')
    op3 = ObjectClassProgramActivityFactory(program_activity_reporting_key='aBcD', program_activity_code=None,
                                            program_activity_name=None)
    # Has a PAC/PAN but not a PARK
    op4 = ObjectClassProgramActivityFactory(program_activity_reporting_key='', program_activity_code='342',
                                            program_activity_name='Test')
    op5 = ObjectClassProgramActivityFactory(program_activity_reporting_key=None, program_activity_code='342',
                                            program_activity_name='Test')

    assert number_of_errors(_FILE, database, models=[op1, op2, op3, op4, op5], submission=sub) == 0

    # Ignored for FY 2026 onward
    sub = SubmissionFactory(submission_id=2, reporting_fiscal_year=2026)
    op1 = ObjectClassProgramActivityFactory(program_activity_reporting_key='', program_activity_code='',
                                            program_activity_name='')
    op2 = ObjectClassProgramActivityFactory(program_activity_reporting_key=None, program_activity_code=None,
                                            program_activity_name=None)

    assert number_of_errors(_FILE, database, models=[op1, op2], submission=sub) == 0


def test_failure(database):
    """ Test failure pre-FY26, each row in file C must contain either PAC/PAN or PARK. """

    sub = SubmissionFactory(submission_id=3, reporting_fiscal_year=2025)
    # No PARK or PAC/PAN
    op1 = ObjectClassProgramActivityFactory(program_activity_reporting_key='', program_activity_code='',
                                            program_activity_name='')
    op2 = ObjectClassProgramActivityFactory(program_activity_reporting_key=None, program_activity_code=None,
                                            program_activity_name=None)

    # No PARK, missing either PAC or PAN but not both
    op3 = ObjectClassProgramActivityFactory(program_activity_reporting_key='', program_activity_code='1234',
                                            program_activity_name='')
    op4 = ObjectClassProgramActivityFactory(program_activity_reporting_key='', program_activity_code='',
                                            program_activity_name='Test')

    assert number_of_errors(_FILE, database, models=[op1, op2, op3, op4], submission=sub) == 4
