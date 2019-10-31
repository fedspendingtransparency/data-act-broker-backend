from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from decimal import Decimal

_FILE = 'c5_award_financial_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'gross_outlay_amount_by_awa_fyb', 'gross_outlays_undelivered_fyb',
                       'gross_outlays_delivered_or_fyb', 'difference', 'uniqueid_TAS', 'uniqueid_PIID', 'uniqueid_FAIN',
                       'uniqueid_URI'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test that calculation passes with equal values and with a null """

    value_one = Decimal('101.23')
    value_two = Decimal('102.34')
    award_fin = AwardFinancialFactory(gross_outlay_amount_by_awa_fyb=value_one + value_two,
                                      gross_outlays_undelivered_fyb=value_one,
                                      gross_outlays_delivered_or_fyb=value_two)
    award_fin_null = AwardFinancialFactory(gross_outlay_amount_by_awa_fyb=value_one,
                                           gross_outlays_undelivered_fyb=None,
                                           gross_outlays_delivered_or_fyb=value_one)

    assert number_of_errors(_FILE, database, models=[award_fin, award_fin_null]) == 0


def test_failure(database):
    """ Test that calculation fails for unequal values """
    value_one = Decimal('101.23')
    value_two = Decimal('102.34')
    award_fin = AwardFinancialFactory(gross_outlay_amount_by_awa_fyb=value_one,
                                      gross_outlays_undelivered_fyb=value_two,
                                      gross_outlays_delivered_or_fyb=value_two)

    assert number_of_errors(_FILE, database, models=[award_fin]) == 1
