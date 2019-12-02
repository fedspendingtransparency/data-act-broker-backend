from random import randint
from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b18_object_class_program_activity'


def test_column_headers(database):
    expected_subset = {'row_number', 'object_class', 'by_direct_reimbursable_fun', 'uniqueid_TAS',
                       'uniqueid_ObjectClass'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset <= actual


def test_skip_three_digits(database):
    check_query(database, ObjectClassProgramActivityFactory(object_class=str(randint(0, 999)).zfill(3),
                by_direct_reimbursable_fun='d'), 0)


def test_skip_blank_flags(database):
    check_query(database, ObjectClassProgramActivityFactory(object_class=randint(1000, 9999),
                by_direct_reimbursable_fun=''), 0)


def test_match_d_value(database):
    check_query(database, ObjectClassProgramActivityFactory(object_class=randint(1000, 1999),
                by_direct_reimbursable_fun='d'), 0)


def test_match_r_value(database):
    check_query(database, ObjectClassProgramActivityFactory(object_class=randint(2000, 2999),
                by_direct_reimbursable_fun='r'), 0)


def test_ignore_all_zero(database):
    check_query(database,
                ObjectClassProgramActivityFactory(object_class=randint(1000, 1999), by_direct_reimbursable_fun='r',
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
                                                  ussgl498100_upward_adjustm_cpe=0, ussgl498200_upward_adjustm_cpe=0),
                0)
    check_query(database,
                ObjectClassProgramActivityFactory(object_class=randint(2000, 2999), by_direct_reimbursable_fun='d',
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
                                                  ussgl498100_upward_adjustm_cpe=0, ussgl498200_upward_adjustm_cpe=0),
                0)


def test_mismatch_d_value(database):
    check_query(database, ObjectClassProgramActivityFactory(object_class=randint(2000, 2999),
                by_direct_reimbursable_fun='d'), 1)


def test_mismatch_r_value(database):
    check_query(database, ObjectClassProgramActivityFactory(object_class=randint(1000, 1999),
                by_direct_reimbursable_fun='r'), 1)


def check_query(db, model, num_expected_errors):
    """ Test that a four digit object class with no flag is a success, and a three digit object class with
        a flag is a success """
    errors = number_of_errors(_FILE, db, models=[model])
    assert errors == num_expected_errors
