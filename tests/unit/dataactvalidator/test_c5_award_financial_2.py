from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from random import randint
from decimal import Decimal

_FILE = 'c5_award_financial_2'


def test_column_headers(database):
    expected_subset = {'row_number', "gross_outlay_amount_by_awa_fyb", "gross_outlays_undelivered_fyb",
	    "gross_outlays_delivered_or_fyb"}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test that calculation passes with equal values and with a null """

    value_one = Decimal(randint(0,100000)) / Decimal(100)
    value_two = Decimal(randint(0,100000)) / Decimal(100)
    award_fin = AwardFinancialFactory(gross_outlay_amount_by_awa_fyb = value_one+value_two,
                                             gross_outlays_undelivered_fyb = value_one,
                                             gross_outlays_delivered_or_fyb = value_two)
    award_fin_null = AwardFinancialFactory(gross_outlay_amount_by_awa_fyb = value_one,
                                                  gross_outlays_undelivered_fyb = None,
                                                  gross_outlays_delivered_or_fyb = value_one)

    assert number_of_errors(_FILE, database, models=[award_fin, award_fin_null]) == 0

def test_failure(database):
    """ Test that calculation fails for unequal values """
    value_one = Decimal(randint(0,100000)) / Decimal(100)
    value_two = Decimal(randint(100001,200000)) / Decimal(100)
    award_fin = AwardFinancialFactory(gross_outlay_amount_by_awa_fyb = value_one,
                                             gross_outlays_undelivered_fyb = value_two,
                                             gross_outlays_delivered_or_fyb = value_two)

    assert number_of_errors(_FILE, database, models=[award_fin]) == 1
