from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b19_object_class_program_activity_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'tas', 'object_class', 'program_activity_code', 'program_activity_name',
                       'by_direct_reimbursable_fun', 'disaster_emergency_fund_code', 'prior_year_adjustment',
                       'uniqueid_TAS', 'uniqueid_ProgramActivityCode', 'uniqueid_ProgramActivityName',
                       'uniqueid_ObjectClass', 'uniqueid_ByDirectReimbursableFundingSource',
                       'uniqueid_DisasterEmergencyFundCode', 'uniqueid_PriorYearAdjustment'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """
        Tests that all combinations of TAS, Object Class, Program Activity, Reimbursable Code, DEFC, and PYA in File B
        (Object Class Program Activity) are unique
    """

    op1 = ObjectClassProgramActivityFactory(display_tas='abcde', object_class='1', program_activity_code='1',
                                            program_activity_name='n', by_direct_reimbursable_fun='r',
                                            disaster_emergency_fund_code='n', prior_year_adjustment='x')

    op2 = ObjectClassProgramActivityFactory(display_tas='abcdef', object_class='1', program_activity_code='1',
                                            program_activity_name='n', by_direct_reimbursable_fun='r',
                                            disaster_emergency_fund_code='n', prior_year_adjustment='x')

    op3 = ObjectClassProgramActivityFactory(display_tas='abcde', object_class='2', program_activity_code='1',
                                            program_activity_name='n', by_direct_reimbursable_fun='r',
                                            disaster_emergency_fund_code='n', prior_year_adjustment='x')

    op4 = ObjectClassProgramActivityFactory(display_tas='abcde', object_class='1', program_activity_code='2',
                                            program_activity_name='n', by_direct_reimbursable_fun='r',
                                            disaster_emergency_fund_code='n', prior_year_adjustment='x')

    op5 = ObjectClassProgramActivityFactory(display_tas='abcde', object_class='1', program_activity_code='1',
                                            program_activity_name='m', by_direct_reimbursable_fun='r',
                                            disaster_emergency_fund_code='n', prior_year_adjustment='x')

    op6 = ObjectClassProgramActivityFactory(display_tas='abcde', object_class='1', program_activity_code='1',
                                            program_activity_name='n', by_direct_reimbursable_fun='d',
                                            disaster_emergency_fund_code='n', prior_year_adjustment='x')

    op7 = ObjectClassProgramActivityFactory(display_tas='abcde', object_class='1', program_activity_code='1',
                                            program_activity_name='n', by_direct_reimbursable_fun='r',
                                            disaster_emergency_fund_code='m', prior_year_adjustment='x')

    op8 = ObjectClassProgramActivityFactory(display_tas='abcde', object_class='1', program_activity_code='1',
                                            program_activity_name='n', by_direct_reimbursable_fun='r',
                                            disaster_emergency_fund_code='n', prior_year_adjustment='a')

    # 2 with the same PARK but no PAC/PAN, ignored
    op9 = ObjectClassProgramActivityFactory(display_tas='abcde', object_class='1', program_activity_code='',
                                            program_activity_name=None, program_activity_reporting_key='abc',
                                            by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                                            prior_year_adjustment='a')

    op10 = ObjectClassProgramActivityFactory(display_tas='abcde', object_class='1', program_activity_code='',
                                             program_activity_name=None, program_activity_reporting_key='abc',
                                             by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                                             prior_year_adjustment='a')

    assert number_of_errors(_FILE, database, models=[op1, op2, op3, op4, op5, op6, op7, op8, op9, op10]) == 0


def test_failure(database):
    """
        Tests that all combinations of TAS, Object Class, Program Activity, Reimbursable Code, DEFC, and PYA in File B
        (Object Class Program Activity) are not unique
    """

    op1 = ObjectClassProgramActivityFactory(display_tas='abcde', object_class='1', program_activity_code='1',
                                            program_activity_name='n', by_direct_reimbursable_fun='r',
                                            disaster_emergency_fund_code='n', prior_year_adjustment='x')

    op2 = ObjectClassProgramActivityFactory(display_tas='AbCdE', object_class='1', program_activity_code='1',
                                            program_activity_name='n', by_direct_reimbursable_fun='r',
                                            disaster_emergency_fund_code='N', prior_year_adjustment='X')

    # object class with extra trailing zeroes treated the same as without
    op3 = ObjectClassProgramActivityFactory(display_tas='abcde', object_class='10', program_activity_code='1',
                                            program_activity_name='n', by_direct_reimbursable_fun='r',
                                            disaster_emergency_fund_code='N', prior_year_adjustment='x')

    assert number_of_errors(_FILE, database, models=[op1, op2, op3]) == 2
