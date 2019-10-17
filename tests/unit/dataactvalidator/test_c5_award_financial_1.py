from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from decimal import Decimal

_FILE = 'c5_award_financial_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'gross_outlay_amount_by_awa_cpe', 'gross_outlays_undelivered_cpe',
                       'gross_outlays_delivered_or_cpe', 'variance'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test that calculation passes with equal values and with a null """

    value_one = Decimal('101.23')
    value_two = Decimal('102.34')
    value_three = Decimal('103.45')
    value_four = Decimal('104.56')
    award_fin = AwardFinancialFactory(gross_outlay_amount_by_awa_cpe=value_one - value_three + value_two - value_four,
                                      gross_outlays_undelivered_cpe=value_one,
                                      gross_outlays_delivered_or_cpe=value_two,
                                      gross_outlays_undelivered_fyb=value_three,
                                      gross_outlays_delivered_or_fyb=value_four)
    award_fin_2 = AwardFinancialFactory(gross_outlay_amount_by_awa_cpe=-value_three + value_two - value_four,
                                        gross_outlays_undelivered_cpe=None,
                                        gross_outlays_delivered_or_cpe=value_two,
                                        gross_outlays_undelivered_fyb=value_three,
                                        gross_outlays_delivered_or_fyb=value_four)
    award_fin_3 = AwardFinancialFactory(gross_outlay_amount_by_awa_cpe=value_one - value_three - value_four,
                                        gross_outlays_undelivered_cpe=value_one,
                                        gross_outlays_delivered_or_cpe=None,
                                        gross_outlays_undelivered_fyb=value_three,
                                        gross_outlays_delivered_or_fyb=value_four)
    award_fin_4 = AwardFinancialFactory(gross_outlay_amount_by_awa_cpe=value_one + value_two - value_four,
                                        gross_outlays_undelivered_cpe=value_one,
                                        gross_outlays_delivered_or_cpe=value_two,
                                        gross_outlays_undelivered_fyb=None,
                                        gross_outlays_delivered_or_fyb=value_four)
    award_fin_5 = AwardFinancialFactory(gross_outlay_amount_by_awa_cpe=value_one - value_three + value_two,
                                        gross_outlays_undelivered_cpe=value_one,
                                        gross_outlays_delivered_or_cpe=value_two,
                                        gross_outlays_undelivered_fyb=value_three,
                                        gross_outlays_delivered_or_fyb=None)

    assert number_of_errors(_FILE, database, models=[award_fin, award_fin_2, award_fin_3, award_fin_4,
                                                     award_fin_5]) == 0


def test_failure(database):
    """ Test that calculation fails for unequal values """
    value_one = Decimal('101.23')
    value_two = Decimal('102.34')
    value_three = Decimal('103.45')
    value_four = Decimal('104.56')
    award_fin = AwardFinancialFactory(gross_outlay_amount_by_awa_cpe=value_one - value_three + value_two - value_four,
                                      gross_outlays_undelivered_cpe=value_one,
                                      gross_outlays_delivered_or_cpe=value_two,
                                      gross_outlays_undelivered_fyb=value_three,
                                      gross_outlays_delivered_or_fyb=None)
    award_fin_2 = AwardFinancialFactory(gross_outlay_amount_by_awa_cpe=value_one + value_two,
                                        gross_outlays_undelivered_cpe=value_one,
                                        gross_outlays_delivered_or_cpe=value_two,
                                        gross_outlays_undelivered_fyb=value_three,
                                        gross_outlays_delivered_or_fyb=value_four)

    assert number_of_errors(_FILE, database, models=[award_fin, award_fin_2]) == 2
