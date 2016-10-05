from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b12_object_class_program_activity_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'by_direct_reimbursable_fun'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test for USSGL 48XX & 49XX (except 487X & 497X) if any one is provided then
    by_direct_reimbursable_fun is not empty """

    op = ObjectClassProgramActivityFactory()
    assert number_of_errors(_FILE, database, models=[op]) == 0

    op = ObjectClassProgramActivityFactory(object_class=1234)
    assert number_of_errors(_FILE, database, models=[op]) == 0


def test_failure(database):
    """ Test for USSGL 48XX & 49XX (except 487X & 497X) if any one is provided then
    by_direct_reimbursable_fun is empty """

    op = ObjectClassProgramActivityFactory(ussgl480100_undelivered_or_fyb=None, by_direct_reimbursable_fun=None,
        object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(ussgl480100_undelivered_or_cpe=None, by_direct_reimbursable_fun=None,
        object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(ussgl483100_undelivered_or_cpe=None, by_direct_reimbursable_fun=None,
        object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(ussgl488100_upward_adjustm_cpe=None, by_direct_reimbursable_fun=None,
        object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(ussgl490100_delivered_orde_fyb=None, by_direct_reimbursable_fun=None,
        object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(ussgl490100_delivered_orde_cpe=None, by_direct_reimbursable_fun=None,
        object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(ussgl493100_delivered_orde_cpe=None, by_direct_reimbursable_fun=None,
        object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(ussgl498100_upward_adjustm_cpe=None, by_direct_reimbursable_fun=None,
        object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(ussgl480200_undelivered_or_fyb=None, by_direct_reimbursable_fun=None,
        object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(ussgl480200_undelivered_or_cpe=None, by_direct_reimbursable_fun=None,
        object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(ussgl483200_undelivered_or_cpe=None, by_direct_reimbursable_fun=None,
        object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(ussgl488200_upward_adjustm_cpe=None, by_direct_reimbursable_fun=None,
        object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(ussgl490200_delivered_orde_cpe=None, by_direct_reimbursable_fun=None,
        object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(ussgl490800_authority_outl_fyb=None, by_direct_reimbursable_fun=None,
        object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(ussgl490800_authority_outl_cpe=None, by_direct_reimbursable_fun=None,
        object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(ussgl498200_upward_adjustm_cpe=None, by_direct_reimbursable_fun=None,
        object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(by_direct_reimbursable_fun=None, object_class = 123)
    assert number_of_errors(_FILE, database, models=[op]) == 1