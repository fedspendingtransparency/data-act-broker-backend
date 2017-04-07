from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactcore.factories.domain import ProgramActivityFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b9_object_class_program_activity'


def test_column_headers(database):
    expected_subset = {'row_number', 'agency_identifier', 'main_account_code', 'program_activity_name',
                       'program_activity_code'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Testing valid program activity name for the corresponding TAS/TAFS as defined in Section 82 of OMB Circular
    A-11. """

    op_1 = ObjectClassProgramActivityFactory(row_number=1, agency_identifier='test', main_account_code='test',
                                             program_activity_name='test', program_activity_code='test')

    op_2 = ObjectClassProgramActivityFactory(row_number=2, agency_identifier='test', main_account_code='test',
                                             program_activity_name='test', program_activity_code='test')

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    assert number_of_errors(_FILE, database, models=[op_1, op_2, pa]) == 0


def test_success_fiscal_year(database):
    """ Testing valid name for FY that matches with budget_year"""

    op = ObjectClassProgramActivityFactory(row_number=1, submission_id='1', agency_identifier='test',
                                           main_account_code='test', program_activity_name='test',
                                           program_activity_code='test')

    pa_1 = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                  account_number='test', program_activity_name='test', program_activity_code='test')

    pa_2 = ProgramActivityFactory(budget_year=2017, agency_id='test2', allocation_transfer_id='test2',
                                  account_number='test2', program_activity_name='test2', program_activity_code='test2')

    submission = SubmissionFactory(submission_id='1', reporting_fiscal_year='2016')

    assert number_of_errors(_FILE, database, models=[op, pa_1, pa_2], submission=submission) == 0


def test_failure_fiscal_year(database):
    """ Testing invalid name for FY, not matches with budget_year"""

    op = ObjectClassProgramActivityFactory(row_number=1, submission_id='1', agency_identifier='test2',
                                           main_account_code='test2', program_activity_name='test2',
                                           program_activity_code='test2')

    pa_1 = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                  account_number='test', program_activity_name='test', program_activity_code='test')

    pa_2 = ProgramActivityFactory(budget_year=2017, agency_id='test2', allocation_transfer_id='test2',
                                  account_number='test2', program_activity_name='test2', program_activity_code='test2')

    submission = SubmissionFactory(submission_id='1', reporting_fiscal_year='2016')

    assert number_of_errors(_FILE, database, models=[op, pa_1, pa_2], submission=submission) == 1


def test_success_unknown_value(database):
    """ Testing valid Unknown/other program activity name with '0000' code """

    op = ObjectClassProgramActivityFactory(row_number=1, agency_identifier='test', main_account_code='test',
                                           program_activity_name='Unknown/Other', program_activity_code='0000')

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    assert number_of_errors(_FILE, database, models=[op, pa]) == 0


def test_success_ignore_blank_program_activity_name(database):
    """ Testing program activity name validation to ignore blanks if monetary sum is 0 """
    op = ObjectClassProgramActivityFactory(row_number=1, beginning_period_of_availa=2016, agency_identifier='test',
                                           main_account_code='test', program_activity_name='',
                                           program_activity_code='test',
                                           deobligations_recov_by_pro_cpe=0, gross_outlay_amount_by_pro_cpe=0,
                                           gross_outlay_amount_by_pro_fyb=0, gross_outlays_delivered_or_cpe=0,
                                           gross_outlays_delivered_or_fyb=0, gross_outlays_undelivered_cpe=0,
                                           gross_outlays_undelivered_fyb=0, obligations_delivered_orde_cpe=0,
                                           obligations_delivered_orde_fyb=0, obligations_incurred_by_pr_cpe=0,
                                           obligations_undelivered_or_cpe=0, obligations_undelivered_or_fyb=0,
                                           ussgl480100_undelivered_or_cpe=0, ussgl480100_undelivered_or_fyb=0,
                                           ussgl480200_undelivered_or_cpe=0, ussgl480200_undelivered_or_fyb=0,
                                           ussgl483100_undelivered_or_cpe=0, ussgl483200_undelivered_or_cpe=0,
                                           ussgl487100_downward_adjus_cpe=0, ussgl487200_downward_adjus_cpe=0,
                                           ussgl488100_upward_adjustm_cpe=0, ussgl488200_upward_adjustm_cpe=0,
                                           ussgl490100_delivered_orde_cpe=0, ussgl490100_delivered_orde_fyb=0,
                                           ussgl490200_delivered_orde_cpe=0, ussgl490800_authority_outl_cpe=0,
                                           ussgl490800_authority_outl_fyb=0, ussgl493100_delivered_orde_cpe=0,
                                           ussgl497100_downward_adjus_cpe=0, ussgl497200_downward_adjus_cpe=0,
                                           ussgl498100_upward_adjustm_cpe=0, ussgl498200_upward_adjustm_cpe=0)

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    assert number_of_errors(_FILE, database, models=[op, pa]) == 0


def test_success_ignore_case(database):
    """ Testing program activity validation to ignore case """

    op = ObjectClassProgramActivityFactory(row_number=1, beginning_period_of_availa=2016, agency_identifier='test',
                                           main_account_code='test', program_activity_name='TEST',
                                           program_activity_code='test')

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    assert number_of_errors(_FILE, database, models=[op, pa]) == 0


def test_failure_program_activity_name(database):
    """ Testing invalid program activity name for the corresponding TAS/TAFS as defined in Section 82 of OMB Circular
    A-11. """

    op_1 = ObjectClassProgramActivityFactory(row_number=1, agency_identifier='test', main_account_code='test',
                                             program_activity_name='test_wrong', program_activity_code='test')

    op_2 = ObjectClassProgramActivityFactory(row_number=1, agency_identifier='test', main_account_code='test',
                                             program_activity_name='test_wrong', program_activity_code='0000')

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    assert number_of_errors(_FILE, database, models=[op_1, op_2, pa]) == 1


def test_failure_program_activity_code(database):
    op_1 = ObjectClassProgramActivityFactory(row_number=1, agency_identifier='test', main_account_code='test',
                                             program_activity_name='test', program_activity_code='test_wrong')

    op_2 = ObjectClassProgramActivityFactory(row_number=1, agency_identifier='test', main_account_code='test',
                                             program_activity_name='Unknown/Other', program_activity_code='123456')

    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    assert number_of_errors(_FILE, database, models=[op_1, op_2, pa]) == 1


def test_failure_empty_activity_name(database):
    """ Testing program activity name validation to not ignore blanks if monetary sum is not 0 """
    pa = ProgramActivityFactory(budget_year=2016, agency_id='test', allocation_transfer_id='test',
                                account_number='test', program_activity_name='test', program_activity_code='test')

    # one monetary amount not 0
    op = ObjectClassProgramActivityFactory(row_number=1, beginning_period_of_availa=2016, agency_identifier='test',
                                           main_account_code='test', program_activity_name='',
                                           program_activity_code='test',
                                           deobligations_recov_by_pro_cpe=2, gross_outlay_amount_by_pro_cpe=0,
                                           gross_outlay_amount_by_pro_fyb=0, gross_outlays_delivered_or_cpe=0,
                                           gross_outlays_delivered_or_fyb=0, gross_outlays_undelivered_cpe=0,
                                           gross_outlays_undelivered_fyb=0, obligations_delivered_orde_cpe=0,
                                           obligations_delivered_orde_fyb=0, obligations_incurred_by_pr_cpe=0,
                                           obligations_undelivered_or_cpe=0, obligations_undelivered_or_fyb=0,
                                           ussgl480100_undelivered_or_cpe=0, ussgl480100_undelivered_or_fyb=0,
                                           ussgl480200_undelivered_or_cpe=0, ussgl480200_undelivered_or_fyb=0,
                                           ussgl483100_undelivered_or_cpe=0, ussgl483200_undelivered_or_cpe=0,
                                           ussgl487100_downward_adjus_cpe=0, ussgl487200_downward_adjus_cpe=0,
                                           ussgl488100_upward_adjustm_cpe=0, ussgl488200_upward_adjustm_cpe=0,
                                           ussgl490100_delivered_orde_cpe=0, ussgl490100_delivered_orde_fyb=0,
                                           ussgl490200_delivered_orde_cpe=0, ussgl490800_authority_outl_cpe=0,
                                           ussgl490800_authority_outl_fyb=0, ussgl493100_delivered_orde_cpe=0,
                                           ussgl497100_downward_adjus_cpe=0, ussgl497200_downward_adjus_cpe=0,
                                           ussgl498100_upward_adjustm_cpe=0, ussgl498200_upward_adjustm_cpe=0)

    assert number_of_errors(_FILE, database, models=[op, pa]) == 1

    # several monetary amounts not 0
    op = ObjectClassProgramActivityFactory(row_number=1, beginning_period_of_availa=2016, agency_identifier='test',
                                           main_account_code='test', program_activity_name='',
                                           program_activity_code='test',
                                           deobligations_recov_by_pro_cpe=2, gross_outlay_amount_by_pro_cpe=0,
                                           gross_outlay_amount_by_pro_fyb=0, gross_outlays_delivered_or_cpe=0,
                                           gross_outlays_delivered_or_fyb=0, gross_outlays_undelivered_cpe=0,
                                           gross_outlays_undelivered_fyb=0, obligations_delivered_orde_cpe=0,
                                           obligations_delivered_orde_fyb=-2, obligations_incurred_by_pr_cpe=0,
                                           obligations_undelivered_or_cpe=0, obligations_undelivered_or_fyb=0,
                                           ussgl480100_undelivered_or_cpe=0, ussgl480100_undelivered_or_fyb=0,
                                           ussgl480200_undelivered_or_cpe=0, ussgl480200_undelivered_or_fyb=-0.4,
                                           ussgl483100_undelivered_or_cpe=0, ussgl483200_undelivered_or_cpe=0,
                                           ussgl487100_downward_adjus_cpe=0.4, ussgl487200_downward_adjus_cpe=0,
                                           ussgl488100_upward_adjustm_cpe=0, ussgl488200_upward_adjustm_cpe=0,
                                           ussgl490100_delivered_orde_cpe=0, ussgl490100_delivered_orde_fyb=0,
                                           ussgl490200_delivered_orde_cpe=0, ussgl490800_authority_outl_cpe=0,
                                           ussgl490800_authority_outl_fyb=0, ussgl493100_delivered_orde_cpe=0,
                                           ussgl497100_downward_adjus_cpe=0, ussgl497200_downward_adjus_cpe=0,
                                           ussgl498100_upward_adjustm_cpe=0, ussgl498200_upward_adjustm_cpe=0)

    assert number_of_errors(_FILE, database, models=[op, pa]) == 1

    # all monetary amounts not 0
    op = ObjectClassProgramActivityFactory(row_number=1, beginning_period_of_availa=2016, agency_identifier='test',
                                           main_account_code='test', program_activity_name='',
                                           program_activity_code='test',
                                           deobligations_recov_by_pro_cpe=2, gross_outlay_amount_by_pro_cpe=100,
                                           gross_outlay_amount_by_pro_fyb=-0.00003, gross_outlays_delivered_or_cpe=10,
                                           gross_outlays_delivered_or_fyb=5, gross_outlays_undelivered_cpe=5,
                                           gross_outlays_undelivered_fyb=5, obligations_delivered_orde_cpe=5,
                                           obligations_delivered_orde_fyb=-2, obligations_incurred_by_pr_cpe=5,
                                           obligations_undelivered_or_cpe=5, obligations_undelivered_or_fyb=5,
                                           ussgl480100_undelivered_or_cpe=5, ussgl480100_undelivered_or_fyb=5,
                                           ussgl480200_undelivered_or_cpe=5, ussgl480200_undelivered_or_fyb=-0.4,
                                           ussgl483100_undelivered_or_cpe=5, ussgl483200_undelivered_or_cpe=5,
                                           ussgl487100_downward_adjus_cpe=0.4, ussgl487200_downward_adjus_cpe=5,
                                           ussgl488100_upward_adjustm_cpe=5, ussgl488200_upward_adjustm_cpe=5,
                                           ussgl490100_delivered_orde_cpe=5, ussgl490100_delivered_orde_fyb=5,
                                           ussgl490200_delivered_orde_cpe=5, ussgl490800_authority_outl_cpe=5,
                                           ussgl490800_authority_outl_fyb=5, ussgl493100_delivered_orde_cpe=5,
                                           ussgl497100_downward_adjus_cpe=5, ussgl497200_downward_adjus_cpe=5,
                                           ussgl498100_upward_adjustm_cpe=5, ussgl498200_upward_adjustm_cpe=5)

    assert number_of_errors(_FILE, database, models=[op, pa]) == 1
