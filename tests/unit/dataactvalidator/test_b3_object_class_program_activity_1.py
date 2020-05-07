from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from decimal import Decimal


_FILE = 'b3_object_class_program_activity_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'obligations_undelivered_or_fyb', 'ussgl480100_undelivered_or_fyb', 'difference',
                       'uniqueid_TAS', 'uniqueid_DEFC', 'uniqueid_ProgramActivityCode', 'uniqueid_ObjectClass'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test that calculation passes with equal values and with a null """

    value = Decimal('100.23')
    ocpa = ObjectClassProgramActivityFactory(obligations_undelivered_or_fyb=value,
                                             ussgl480100_undelivered_or_fyb=value)
    ocpa_null = ObjectClassProgramActivityFactory(obligations_undelivered_or_fyb=0,
                                                  ussgl480100_undelivered_or_fyb=None)

    assert number_of_errors(_FILE, database, models=[ocpa, ocpa_null]) == 0


def test_failure(database):
    """ Test that calculation fails for unequal values """
    ocpa = ObjectClassProgramActivityFactory(obligations_undelivered_or_fyb=Decimal('101.23'),
                                             ussgl480100_undelivered_or_fyb=Decimal('102.34'))

    assert number_of_errors(_FILE, database, models=[ocpa]) == 1
