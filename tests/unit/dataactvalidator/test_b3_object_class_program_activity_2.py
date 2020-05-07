from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from decimal import Decimal


_FILE = 'b3_object_class_program_activity_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'obligations_undelivered_or_cpe', 'ussgl480100_undelivered_or_cpe',
                       'ussgl483100_undelivered_or_cpe', 'ussgl488100_upward_adjustm_cpe', 'difference', 'uniqueid_TAS',
                       'uniqueid_DEFC', 'uniqueid_ProgramActivityCode', 'uniqueid_ObjectClass'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test that calculation passes with equal values and with a null """

    value_one = Decimal('100.00')
    value_two = Decimal('200.00')
    value_three = Decimal('200.00')
    ocpa = ObjectClassProgramActivityFactory(obligations_undelivered_or_cpe=value_one + value_two + value_three,
                                             ussgl480100_undelivered_or_cpe=value_one,
                                             ussgl483100_undelivered_or_cpe=value_two,
                                             ussgl488100_upward_adjustm_cpe=value_three)
    ocpa_null = ObjectClassProgramActivityFactory(obligations_undelivered_or_cpe=value_one,
                                                  ussgl480100_undelivered_or_cpe=None,
                                                  ussgl483100_undelivered_or_cpe=None,
                                                  ussgl488100_upward_adjustm_cpe=value_one)

    assert number_of_errors(_FILE, database, models=[ocpa, ocpa_null]) == 0


def test_failure(database):
    """ Test that calculation fails for unequal values """
    value = Decimal('500.00')
    value2 = Decimal('100.00')
    ocpa = ObjectClassProgramActivityFactory(obligations_undelivered_or_cpe=value,
                                             ussgl480100_undelivered_or_cpe=value2,
                                             ussgl483100_undelivered_or_cpe=value2,
                                             ussgl488100_upward_adjustm_cpe=value2)

    assert number_of_errors(_FILE, database, models=[ocpa]) == 1
