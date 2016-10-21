from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from random import randint
from decimal import Decimal

_FILE = 'c6_award_financial_2'

def test_column_headers(database):
    expected_subset = {'row_number', "gross_outlays_undelivered_fyb", "ussgl480200_undelivered_or_fyb"}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test that calculation passes with equal values and with a null """

    value_one = Decimal(randint(0,100000)) / Decimal(100)
    award_fin = AwardFinancialFactory(gross_outlays_undelivered_fyb = value_one,
                                 ussgl480200_undelivered_or_fyb = value_one)
    award_fin_null = AwardFinancialFactory(gross_outlays_undelivered_fyb = 0,
                                      ussgl480200_undelivered_or_fyb = None)

    assert number_of_errors(_FILE, database, models=[award_fin, award_fin_null]) == 0

def test_failure(database):
    """ Test that calculation fails for unequal values """
    value_one = Decimal(randint(0,100000)) / Decimal(100)
    value_two = Decimal(randint(100001,200000)) / Decimal(100)
    award_fin = AwardFinancialFactory(gross_outlays_undelivered_fyb = value_one,
                                 ussgl480200_undelivered_or_fyb = value_two)

    assert number_of_errors(_FILE, database, models=[award_fin]) == 1
