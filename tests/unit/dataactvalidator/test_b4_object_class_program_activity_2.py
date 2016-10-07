from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from random import randint
from decimal import Decimal

_FILE = 'b4_object_class_program_activity_2'


def test_column_headers(database):
    expected_subset = {'row_number', "obligations_delivered_orde_cpe", "ussgl490100_delivered_orde_cpe",
	    "ussgl493100_delivered_orde_cpe", "ussgl497100_downward_adjus_cpe", "ussgl498100_upward_adjustm_cpe"}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test that calculation passes with equal values and with a null """

    value_one = Decimal(randint(0,100000)) / Decimal(100)
    value_two = Decimal(randint(0,100000)) / Decimal(100)
    value_three = Decimal(randint(0,100000)) / Decimal(100)
    value_four = Decimal(randint(0,100000)) / Decimal(100)
    ocpa = ObjectClassProgramActivityFactory(obligations_delivered_orde_cpe = value_one+value_two+value_three+value_four,
                                             ussgl490100_delivered_orde_cpe = value_one,
                                             ussgl493100_delivered_orde_cpe = value_two,
                                             ussgl497100_downward_adjus_cpe = value_three,
                                             ussgl498100_upward_adjustm_cpe = value_four)
    ocpa_null = ObjectClassProgramActivityFactory(obligations_delivered_orde_cpe = value_one+value_two+value_three,
                                                  ussgl490100_delivered_orde_cpe = None,
                                                  ussgl493100_delivered_orde_cpe = value_one,
                                                  ussgl497100_downward_adjus_cpe = value_two,
                                                  ussgl498100_upward_adjustm_cpe = value_three)

    assert number_of_errors(_FILE, database, models=[ocpa, ocpa_null]) == 0

def test_failure(database):
    """ Test that calculation fails for unequal values """
    value = Decimal(randint(0,100000)) / Decimal(100)
    value2 = Decimal(randint(100001,200000)) / Decimal(100)
    ocpa = ObjectClassProgramActivityFactory(obligations_delivered_orde_cpe = value,
                                             ussgl490100_delivered_orde_cpe = value2,
                                             ussgl493100_delivered_orde_cpe = value2,
                                             ussgl497100_downward_adjus_cpe = value2,
                                             ussgl498100_upward_adjustm_cpe = value2)

    assert number_of_errors(_FILE, database, models=[ocpa]) == 1
