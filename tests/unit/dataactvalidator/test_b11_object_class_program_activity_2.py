from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactcore.factories.domain import ObjectClassFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b11_object_class_program_activity_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'object_class', 'uniqueid_TAS', 'uniqueid_ObjectClass'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test valid object class code (3 digits) """

    op = ObjectClassProgramActivityFactory(object_class='object_class')
    oc = ObjectClassFactory(object_class_code='object_class')

    assert number_of_errors(_FILE, database, models=[op, oc]) == 0


def test_success_all_zero(database):
    """ Test not returning a warning on '000' when all monetary values are 0 """

    op = ObjectClassProgramActivityFactory(object_class='000', deobligations_recov_by_pro_cpe=0,
                                           gross_outlay_amount_by_pro_cpe=0, gross_outlay_amount_by_pro_fyb=0,
                                           gross_outlays_delivered_or_cpe=0, gross_outlays_delivered_or_fyb=0,
                                           gross_outlays_undelivered_cpe=0, gross_outlays_undelivered_fyb=0,
                                           obligations_delivered_orde_cpe=0, obligations_delivered_orde_fyb=0,
                                           obligations_incurred_by_pr_cpe=0, obligations_undelivered_or_cpe=0,
                                           obligations_undelivered_or_fyb=0, ussgl480100_undelivered_or_cpe=0,
                                           ussgl480100_undelivered_or_fyb=0, ussgl480200_undelivered_or_cpe=0,
                                           ussgl480200_undelivered_or_fyb=0, ussgl483100_undelivered_or_cpe=0,
                                           ussgl483200_undelivered_or_cpe=0, ussgl487100_downward_adjus_cpe=0,
                                           ussgl487200_downward_adjus_cpe=0, ussgl488100_upward_adjustm_cpe=0,
                                           ussgl488200_upward_adjustm_cpe=0, ussgl490100_delivered_orde_cpe=0,
                                           ussgl490100_delivered_orde_fyb=0, ussgl490200_delivered_orde_cpe=0,
                                           ussgl490800_authority_outl_cpe=0, ussgl490800_authority_outl_fyb=0,
                                           ussgl493100_delivered_orde_cpe=0, ussgl497100_downward_adjus_cpe=0,
                                           ussgl497200_downward_adjus_cpe=0, ussgl498100_upward_adjustm_cpe=0,
                                           ussgl498200_upward_adjustm_cpe=0)
    assert number_of_errors(_FILE, database, models=[op]) == 0


def test_failure(database):
    """ Test invalid object class code (3 digits) """

    # This should return because if it's '0000' '000', '00', '0' a warning should be returned
    op = ObjectClassProgramActivityFactory(object_class='0000')
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(object_class='000')
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(object_class='00')
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(object_class='0')
    assert number_of_errors(_FILE, database, models=[op]) == 1


def test_fail_nonzero(database):
    """ Test returning a warning on '000' when not all monetary values are 0 """

    # even one non-zero should return a warning
    op = ObjectClassProgramActivityFactory(object_class='000', deobligations_recov_by_pro_cpe=1,
                                           gross_outlay_amount_by_pro_cpe=0, gross_outlay_amount_by_pro_fyb=0,
                                           gross_outlays_delivered_or_cpe=0, gross_outlays_delivered_or_fyb=0,
                                           gross_outlays_undelivered_cpe=0, gross_outlays_undelivered_fyb=0,
                                           obligations_delivered_orde_cpe=0, obligations_delivered_orde_fyb=0,
                                           obligations_incurred_by_pr_cpe=0, obligations_undelivered_or_cpe=0,
                                           obligations_undelivered_or_fyb=0, ussgl480100_undelivered_or_cpe=0,
                                           ussgl480100_undelivered_or_fyb=0, ussgl480200_undelivered_or_cpe=0,
                                           ussgl480200_undelivered_or_fyb=0, ussgl483100_undelivered_or_cpe=0,
                                           ussgl483200_undelivered_or_cpe=0, ussgl487100_downward_adjus_cpe=0,
                                           ussgl487200_downward_adjus_cpe=0, ussgl488100_upward_adjustm_cpe=0,
                                           ussgl488200_upward_adjustm_cpe=0, ussgl490100_delivered_orde_cpe=0,
                                           ussgl490100_delivered_orde_fyb=0, ussgl490200_delivered_orde_cpe=0,
                                           ussgl490800_authority_outl_cpe=0, ussgl490800_authority_outl_fyb=0,
                                           ussgl493100_delivered_orde_cpe=0, ussgl497100_downward_adjus_cpe=0,
                                           ussgl497200_downward_adjus_cpe=0, ussgl498100_upward_adjustm_cpe=0,
                                           ussgl498200_upward_adjustm_cpe=0)
    assert number_of_errors(_FILE, database, models=[op]) == 1
