from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactcore.factories.domain import ProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b9_object_class_program_activity'


def test_column_headers(database):
    expected_subset = {'row_number', 'beginning_period_of_availa', 'agency_identifier', 'allocation_transfer_agency',
                       'main_account_code', 'program_activity_name', 'program_activity_code'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Testing valid program activity name for the corresponding TAS/TAFS as defined in Section 82 of OMB Circular
    A-11. """

    op_1 = ObjectClassProgramActivityFactory(row_number=1, beginning_period_of_availa=2016, agency_identifier='test',
                                             allocation_transfer_agency='test', main_account_code='test',
                                             program_activity_name='test', program_activity_code='test')

    op_2 = ObjectClassProgramActivityFactory(row_number=2, beginning_period_of_availa=2016, agency_identifier='test',
                                             allocation_transfer_agency='test', main_account_code='test',
                                             program_activity_name='test', program_activity_code='test')

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    assert number_of_errors(_FILE, database, models=[op_1, op_2, pa]) == 0


def test_success_mismatched_year(database):
    op = ObjectClassProgramActivityFactory(row_number=1, beginning_period_of_availa=2017, agency_identifier='test',
                                           allocation_transfer_agency='test', main_account_code='test',
                                           program_activity_name='test', program_activity_code='test')

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    assert number_of_errors(_FILE, database, models=[op, pa]) == 0


def test_success_unknown_value(database):
    """ Testing valid Unknown/other program activity name with '0000' code """

    op = ObjectClassProgramActivityFactory(row_number=1, beginning_period_of_availa=2016, agency_identifier='test',
                                           allocation_transfer_agency='test', main_account_code='test',
                                           program_activity_name='Unknown/Other', program_activity_code='0000')

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    assert number_of_errors(_FILE, database, models=[op, pa]) == 0


def test_success_ignore_case(database):
    """ Testing program activity validation to ignore case """

    op = ObjectClassProgramActivityFactory(row_number=1, beginning_period_of_availa=2016, agency_identifier='test',
                                           allocation_transfer_agency='test', main_account_code='test',
                                           program_activity_name='test', program_activity_code='test')

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='TEST', program_activity_code='test')

    assert number_of_errors(_FILE, database, models=[op, pa]) == 0


def test_failure_program_activity_name(database):
    """ Testing invalid program activity name for the corresponding TAS/TAFS as defined in Section 82 of OMB Circular
    A-11. """

    op_1 = ObjectClassProgramActivityFactory(row_number=1, beginning_period_of_availa=2016, agency_identifier='test',
                                             allocation_transfer_agency='test', main_account_code='test',
                                             program_activity_name='test_wrong', program_activity_code='test')

    op_2 = ObjectClassProgramActivityFactory(row_number=1, beginning_period_of_availa=2016, agency_identifier='test',
                                             allocation_transfer_agency='test', main_account_code='test',
                                             program_activity_name='test_wrong', program_activity_code='0000')

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    assert number_of_errors(_FILE, database, models=[op_1, op_2, pa]) == 1


def test_failure_program_activity_code(database):
    op_1 = ObjectClassProgramActivityFactory(row_number=1, beginning_period_of_availa=2016, agency_identifier='test',
                                             allocation_transfer_agency='test', main_account_code='test',
                                             program_activity_name='test', program_activity_code='test_wrong')

    op_2 = ObjectClassProgramActivityFactory(row_number=1, beginning_period_of_availa=2016, agency_identifier='test',
                                             allocation_transfer_agency='test', main_account_code='test',
                                             program_activity_name='Unknown/Other', program_activity_code='123456')

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    assert number_of_errors(_FILE, database, models=[op_1, op_2, pa]) == 1
