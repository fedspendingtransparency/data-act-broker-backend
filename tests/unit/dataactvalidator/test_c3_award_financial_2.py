from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c3_award_financial_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'obligations_undelivered_or_cpe', 'ussgl480100_undelivered_or_cpe',
                       'ussgl483100_undelivered_or_cpe', 'ussgl488100_upward_adjustm_cpe', 'difference', 'uniqueid_TAS',
                       'uniqueid_DisasterEmergencyFundCode', 'uniqueid_PIID', 'uniqueid_FAIN', 'uniqueid_URI'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ ObligationsUndeliveredOrdersUnpaidTotal in File C = USSGL 4801 + 4881 in File C for the same date context
        (CPE) and TAS/DEFC combination
    """

    af = AwardFinancialFactory(obligations_undelivered_or_cpe=None, ussgl480100_undelivered_or_cpe=None,
                               ussgl483100_undelivered_or_cpe=None, ussgl488100_upward_adjustm_cpe=None)

    assert number_of_errors(_FILE, database, models=[af]) == 0

    af = AwardFinancialFactory(obligations_undelivered_or_cpe=3, ussgl480100_undelivered_or_cpe=1,
                               ussgl483100_undelivered_or_cpe=1, ussgl488100_upward_adjustm_cpe=1)

    assert number_of_errors(_FILE, database, models=[af]) == 0


def test_failure(database):
    """ ObligationsUndeliveredOrdersUnpaidTotal in File C != USSGL 4801 + 4881 in File C for the same date context
        (CPE) and TAS/DEFC combination
    """

    af = AwardFinancialFactory(obligations_undelivered_or_cpe=1, ussgl480100_undelivered_or_cpe=None,
                               ussgl483100_undelivered_or_cpe=None, ussgl488100_upward_adjustm_cpe=None)

    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(obligations_undelivered_or_cpe=1, ussgl480100_undelivered_or_cpe=1,
                               ussgl483100_undelivered_or_cpe=1, ussgl488100_upward_adjustm_cpe=1)

    assert number_of_errors(_FILE, database, models=[af]) == 1
