from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from decimal import Decimal


_FILE = 'c18_award_financial'


def test_column_headers(database):
    expected_subset = {'row_number', 'deobligations_recov_by_awa_cpe', 'ussgl487100_downward_adjus_cpe',
                       'ussgl487200_downward_adjus_cpe', 'ussgl497100_downward_adjus_cpe',
                       'ussgl497200_downward_adjus_cpe', 'difference', 'uniqueid_TAS', 'uniqueid_PIID', 'uniqueid_FAIN',
                       'uniqueid_URI'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test that calculation passes with equal values and with a null """

    value_one = Decimal('101.23')
    value_two = Decimal('102.34')
    value_three = Decimal('103.45')
    value_four = Decimal('104.56')
    award_fin = AwardFinancialFactory(deobligations_recov_by_awa_cpe=value_one + value_two + value_three + value_four,
                                      ussgl487100_downward_adjus_cpe=value_one,
                                      ussgl487200_downward_adjus_cpe=value_two,
                                      ussgl497100_downward_adjus_cpe=value_three,
                                      ussgl497200_downward_adjus_cpe=value_four)
    # Ignore if something is null
    award_fin_null = AwardFinancialFactory(deobligations_recov_by_awa_cpe=value_one + value_two + value_three + 1,
                                           ussgl487100_downward_adjus_cpe=None,
                                           ussgl487200_downward_adjus_cpe=value_one,
                                           ussgl497100_downward_adjus_cpe=value_two,
                                           ussgl497200_downward_adjus_cpe=value_three)

    assert number_of_errors(_FILE, database, models=[award_fin, award_fin_null]) == 0


def test_failure(database):
    """ Test that calculation fails for unequal values """
    value_one = Decimal('101.23')
    value_two = Decimal('102.34')
    award_fin = AwardFinancialFactory(deobligations_recov_by_awa_cpe=value_one,
                                      ussgl487100_downward_adjus_cpe=value_two,
                                      ussgl487200_downward_adjus_cpe=value_two,
                                      ussgl497100_downward_adjus_cpe=value_two,
                                      ussgl497200_downward_adjus_cpe=value_two)

    assert number_of_errors(_FILE, database, models=[award_fin]) == 1
