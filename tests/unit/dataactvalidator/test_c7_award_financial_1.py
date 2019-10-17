from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from decimal import Decimal

_FILE = 'c7_award_financial_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'gross_outlays_delivered_or_cpe', 'ussgl490200_delivered_orde_cpe',
                       'ussgl490800_authority_outl_cpe', 'ussgl498200_upward_adjustm_cpe', 'variance'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test that calculation passes with equal values and with a null """

    value_one = Decimal('101.23')
    value_two = Decimal('102.34')
    value_three = Decimal('103.45')
    award_fin = AwardFinancialFactory(gross_outlays_delivered_or_cpe=value_one + value_two + value_three,
                                      ussgl490200_delivered_orde_cpe=value_one,
                                      ussgl490800_authority_outl_cpe=value_two,
                                      ussgl498200_upward_adjustm_cpe=value_three)
    award_fin_null = AwardFinancialFactory(gross_outlays_delivered_or_cpe=value_one + value_two,
                                           ussgl490200_delivered_orde_cpe=None,
                                           ussgl490800_authority_outl_cpe=value_one,
                                           ussgl498200_upward_adjustm_cpe=value_two)

    assert number_of_errors(_FILE, database, models=[award_fin, award_fin_null]) == 0


def test_failure(database):
    """ Test that calculation fails for unequal values """
    value_one = Decimal('101.23')
    value_two = Decimal('102.34')
    award_fin = AwardFinancialFactory(gross_outlays_delivered_or_cpe=value_one,
                                      ussgl490200_delivered_orde_cpe=value_two,
                                      ussgl483200_undelivered_or_cpe=value_two,
                                      ussgl498200_upward_adjustm_cpe=value_two)

    assert number_of_errors(_FILE, database, models=[award_fin]) == 1
