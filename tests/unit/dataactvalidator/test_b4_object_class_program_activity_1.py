from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from random import randint
from decimal import Decimal

_FILE = 'b4_object_class_program_activity_1'


def test_column_headers(database):
    expected_subset = {'row_number', "obligations_delivered_orde_fyb", "ussgl490100_delivered_orde_fyb"}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test that calculation passes with equal values and with a null """

    value = Decimal(randint(0,100000)) / Decimal(100)
    ocpa = ObjectClassProgramActivityFactory(obligations_delivered_orde_fyb = value,
                                             ussgl490100_delivered_orde_fyb = value)
    ocpa_null = ObjectClassProgramActivityFactory(obligations_delivered_orde_fyb = 0,
                                                  ussgl490100_delivered_orde_fyb = None)

    assert number_of_errors(_FILE, database, models=[ocpa, ocpa_null]) == 0

def test_failure(database):
    """ Test that calculation fails for unequal values """
    value = Decimal(randint(0,100000)) / Decimal(100)
    value2 = Decimal(randint(100001,200000)) / Decimal(100)
    ocpa = ObjectClassProgramActivityFactory(obligations_delivered_orde_fyb = value,
                                             ussgl490100_delivered_orde_fyb = value2)

    assert number_of_errors(_FILE, database, models=[ocpa]) == 1
