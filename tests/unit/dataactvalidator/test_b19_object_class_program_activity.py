from dataactcore.models.stagingModels import ObjectClassProgramActivity
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b19_object_class_program_activity'


def test_column_headers(database):
    expected_subset = {'row_number', 'beginning_period_of_availa', 'ending_period_of_availabil',
                       'agency_identifier', 'allocation_transfer_agency', 'availability_type_code',
                       'main_account_code', 'sub_account_code', 'object_class', 'program_activity_code',
                       'by_direct_reimbursable_fun', 'disaster_emergency_fund_code', 'uniqueid_TAS',
                       'uniqueid_ProgramActivityCode', 'uniqueid_ProgramActivityName', 'uniqueid_ObjectClass',
                       'uniqueid_ByDirectReimbursableFundingSource', 'uniqueid_DisasterEmergencyFundCode'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that all combinations of TAS, Object Class, Program Activity, Reimbursable Code, and DEFC in File B
        (Object Class Program Activity) are unique
    """

    op1 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                     ending_period_of_availabil='1', agency_identifier='1',
                                     allocation_transfer_agency='1', availability_type_code='1',
                                     main_account_code='1', sub_account_code='1', object_class='1',
                                     program_activity_code='1', program_activity_name='n',
                                     by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n')

    op2 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='2',
                                     ending_period_of_availabil='1', agency_identifier='1',
                                     allocation_transfer_agency='1', availability_type_code='1',
                                     main_account_code='1', sub_account_code='1', object_class='1',
                                     program_activity_code='1', program_activity_name='n',
                                     by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n')

    op3 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                     ending_period_of_availabil='2', agency_identifier='1',
                                     allocation_transfer_agency='1', availability_type_code='1',
                                     main_account_code='1', sub_account_code='1', object_class='1',
                                     program_activity_code='1', program_activity_name='n',
                                     by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n')

    op4 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                     ending_period_of_availabil='1', agency_identifier='2',
                                     allocation_transfer_agency='1', availability_type_code='1',
                                     main_account_code='1', sub_account_code='1', object_class='1',
                                     program_activity_code='1', program_activity_name='n',
                                     by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n')

    op5 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                     ending_period_of_availabil='1', agency_identifier='1',
                                     allocation_transfer_agency='2', availability_type_code='1',
                                     main_account_code='1', sub_account_code='1', object_class='1',
                                     program_activity_code='1', program_activity_name='n',
                                     by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n')

    op6 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                     ending_period_of_availabil='1', agency_identifier='1',
                                     allocation_transfer_agency='1', availability_type_code='2',
                                     main_account_code='1', sub_account_code='1', object_class='1',
                                     program_activity_code='1', program_activity_name='n',
                                     by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n')

    op7 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                     ending_period_of_availabil='1', agency_identifier='1',
                                     allocation_transfer_agency='1', availability_type_code='1',
                                     main_account_code='2', sub_account_code='1', object_class='1',
                                     program_activity_code='1', program_activity_name='n',
                                     by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n')

    op8 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                     ending_period_of_availabil='1', agency_identifier='1',
                                     allocation_transfer_agency='1', availability_type_code='1',
                                     main_account_code='1', sub_account_code='2', object_class='1',
                                     program_activity_code='1', program_activity_name='n',
                                     by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n')

    op9 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                     ending_period_of_availabil='1', agency_identifier='1',
                                     allocation_transfer_agency='1', availability_type_code='1',
                                     main_account_code='1', sub_account_code='1', object_class='2',
                                     program_activity_code='1', program_activity_name='n',
                                     by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n')

    op10 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                      ending_period_of_availabil='1', agency_identifier='1',
                                      allocation_transfer_agency='1', availability_type_code='1',
                                      main_account_code='1', sub_account_code='1', object_class='1',
                                      program_activity_code='2', program_activity_name='n',
                                      by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n')

    op11 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                      ending_period_of_availabil='1', agency_identifier='1',
                                      allocation_transfer_agency='1', availability_type_code='1',
                                      main_account_code='1', sub_account_code='1', object_class='1',
                                      program_activity_code='1', program_activity_name='m',
                                      by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n')

    op12 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                      ending_period_of_availabil='1', agency_identifier='1',
                                      allocation_transfer_agency='1', availability_type_code='1',
                                      main_account_code='1', sub_account_code='1', object_class='1',
                                      program_activity_code='1', program_activity_name='n',
                                      by_direct_reimbursable_fun='d', disaster_emergency_fund_code='n')

    # Same values but a different DEFC
    op13 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                      ending_period_of_availabil='1', agency_identifier='1',
                                      allocation_transfer_agency='1', availability_type_code='1',
                                      main_account_code='1', sub_account_code='1', object_class='1',
                                      program_activity_code='1', program_activity_name='n',
                                      by_direct_reimbursable_fun='d', disaster_emergency_fund_code='m')

    assert number_of_errors(_FILE, database, models=[op1, op2, op3, op4, op5, op6, op7, op8, op9, op10, op11,
                                                     op12, op13]) == 0


def test_optionals(database):
    """ Tests that all combinations of TAS, Object Class, Reimbursable Code, and DEFC in File B (Object Class Program
        Activity) are not unique, while omitting an optional field to check that there is still a match
    """

    op1 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                     ending_period_of_availabil='1', agency_identifier='1',
                                     availability_type_code='1', main_account_code='1', sub_account_code='1',
                                     object_class='1', program_activity_code='1', program_activity_name='n',
                                     by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n')

    op2 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                     ending_period_of_availabil='1', agency_identifier='1',
                                     availability_type_code='1', main_account_code='1', sub_account_code='1',
                                     object_class='1', program_activity_code='1', program_activity_name='n',
                                     by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n')

    assert number_of_errors(_FILE, database, models=[op1, op2]) == 1


def test_failure(database):
    """ Tests that all combinations of TAS, Object Class, Program Activity, Reimbursable Code, and DEFC in File B
        (Object Class Program Activity) are not unique
    """

    op1 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                     ending_period_of_availabil='1', agency_identifier='1',
                                     allocation_transfer_agency='1', availability_type_code='1',
                                     main_account_code='1', sub_account_code='1', object_class='1',
                                     program_activity_code='1', program_activity_name='n',
                                     by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n')

    op2 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                     ending_period_of_availabil='1', agency_identifier='1',
                                     allocation_transfer_agency='1', availability_type_code='1',
                                     main_account_code='1', sub_account_code='1', object_class='1',
                                     program_activity_code='1', program_activity_name='n',
                                     by_direct_reimbursable_fun='r', disaster_emergency_fund_code='N')

    # object class with extra trailing zeroes treated the same as without
    op3 = ObjectClassProgramActivity(job_id=1, row_number=1, beginning_period_of_availa='1',
                                     ending_period_of_availabil='1', agency_identifier='1',
                                     allocation_transfer_agency='1', availability_type_code='1',
                                     main_account_code='1', sub_account_code='1', object_class='10',
                                     program_activity_code='1', program_activity_name='n',
                                     by_direct_reimbursable_fun='r', disaster_emergency_fund_code='N')

    assert number_of_errors(_FILE, database, models=[op1, op2, op3]) == 2
