from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from random import randint
from decimal import Decimal

_FILE = 'c6_award_financial_1'

def test_column_headers(database):
    expected_subset = {'row_number', "gross_outlays_undelivered_cpe", "ussgl480200_undelivered_or_cpe",
	    "ussgl483200_undelivered_or_cpe", "ussgl487200_downward_adjus_cpe", "ussgl488200_upward_adjustm_cpe"}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test that calculation passes with equal values and with a null """

    value_one = Decimal(randint(0,100000)) / Decimal(100)
    value_two = Decimal(randint(0,100000)) / Decimal(100)
    value_three = Decimal(randint(0,100000)) / Decimal(100)
    value_four = Decimal(randint(0,100000)) / Decimal(100)
    award_fin = AwardFinancialFactory(gross_outlays_undelivered_cpe = value_one + value_two + value_three + value_four,
                                 ussgl480200_undelivered_or_cpe = value_one,
                                 ussgl483200_undelivered_or_cpe = value_two,
                                 ussgl487200_downward_adjus_cpe = value_three,
                                 ussgl488200_upward_adjustm_cpe = value_four)
    award_fin_null = AwardFinancialFactory(gross_outlays_undelivered_cpe = value_one + value_two + value_three,
                                      ussgl480200_undelivered_or_cpe = None,
                                      ussgl483200_undelivered_or_cpe = value_one,
                                      ussgl487200_downward_adjus_cpe = value_two,
                                      ussgl488200_upward_adjustm_cpe = value_three)

    assert number_of_errors(_FILE, database, models=[award_fin, award_fin_null]) == 0

def test_failure(database):
    """ Test that calculation fails for unequal values """
    value_one = Decimal(randint(0,100000)) / Decimal(100)
    value_two = Decimal(randint(100001,200000)) / Decimal(100)
    award_fin = AwardFinancialFactory(gross_outlays_undelivered_cpe = value_one,
                                 ussgl480200_undelivered_or_cpe = value_two,
                                 ussgl483200_undelivered_or_cpe = value_two,
                                 ussgl487200_downward_adjus_cpe = value_two,
                                 ussgl488200_upward_adjustm_cpe = value_two)

    assert number_of_errors(_FILE, database, models=[award_fin]) == 1
