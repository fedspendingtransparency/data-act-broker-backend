from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c4_award_financial_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'obligations_delivered_orde_fyb', 'ussgl490100_delivered_orde_fyb'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ ObligationsDeliveredOrdersUnpaidTotal in File C = USSGL 4901 + 4981 in File C for the same date context
    (FYB) """

    af = AwardFinancialFactory(obligations_delivered_orde_fyb=None, ussgl490100_delivered_orde_fyb=None)

    assert number_of_errors(_FILE, database, models=[af]) == 0

    af = AwardFinancialFactory(obligations_delivered_orde_fyb=1, ussgl490100_delivered_orde_fyb=1)

    assert number_of_errors(_FILE, database, models=[af]) == 0


def test_failure(database):
    """ ObligationsDeliveredOrdersUnpaidTotal in File C = USSGL 4901 + 4981 in File C for the same date context
    (FYB) """

    af = AwardFinancialFactory(obligations_delivered_orde_fyb=1, ussgl490100_delivered_orde_fyb=None)

    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(obligations_delivered_orde_fyb=1, ussgl490100_delivered_orde_fyb=2)

    assert number_of_errors(_FILE, database, models=[af]) == 1