from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "c3_award_financial_1"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "obligations_undelivered_or_fyb",
        "ussgl480100_undelivered_or_fyb",
        "difference",
        "uniqueid_TAS",
        "uniqueid_DisasterEmergencyFundCode",
        "uniqueid_PriorYearAdjustment",
        "uniqueid_PIID",
        "uniqueid_FAIN",
        "uniqueid_URI",
    }
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ObligationsUndeliveredOrdersUnpaidTotal in File C = USSGL 4801 in File C for the same date context
    (FYB) and TAS/DEFC/PYA combination
    """

    af = AwardFinancialFactory(obligations_undelivered_or_fyb=None, ussgl480100_undelivered_or_fyb=None)

    assert number_of_errors(_FILE, database, models=[af]) == 0

    af = AwardFinancialFactory(obligations_undelivered_or_fyb=1, ussgl480100_undelivered_or_fyb=1)

    assert number_of_errors(_FILE, database, models=[af]) == 0


def test_failure(database):
    """ObligationsUndeliveredOrdersUnpaidTotal in File C != USSGL 4801 in File C for the same date context
    (FYB) and TAS/DEFC/PYA combination
    """

    af = AwardFinancialFactory(obligations_undelivered_or_fyb=1, ussgl480100_undelivered_or_fyb=None)

    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(obligations_undelivered_or_fyb=1, ussgl480100_undelivered_or_fyb=2)

    assert number_of_errors(_FILE, database, models=[af]) == 1
