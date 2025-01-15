from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c4_award_financial_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'obligations_delivered_orde_cpe', 'ussgl490100_delivered_orde_cpe',
                       'ussgl490110_rein_deliv_ord_cpe', 'ussgl493100_delivered_orde_cpe',
                       'ussgl498100_upward_adjustm_cpe', 'difference', 'uniqueid_TAS',
                       'uniqueid_DisasterEmergencyFundCode', 'uniqueid_PriorYearAdjustment', 'uniqueid_PIID',
                       'uniqueid_FAIN', 'uniqueid_URI'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ ObligationsDeliveredOrdersUnpaidTotal in File C = USSGL 4901 + 490110 + 4931 + 4981 in File C for the same date
        context and TAS/DEFC/PYA combination (CPE)
    """

    af = AwardFinancialFactory(obligations_delivered_orde_cpe=None, ussgl490100_delivered_orde_cpe=None,
                               ussgl493100_delivered_orde_cpe=None, ussgl497100_downward_adjus_cpe=None,
                               ussgl498100_upward_adjustm_cpe=None, ussgl490110_rein_deliv_ord_cpe=None)

    assert number_of_errors(_FILE, database, models=[af]) == 0

    af = AwardFinancialFactory(obligations_delivered_orde_cpe=4, ussgl490100_delivered_orde_cpe=1,
                               ussgl493100_delivered_orde_cpe=1, ussgl498100_upward_adjustm_cpe=1,
                               ussgl490110_rein_deliv_ord_cpe=1)

    assert number_of_errors(_FILE, database, models=[af]) == 0


def test_failure(database):
    """ ObligationsDeliveredOrdersUnpaidTotal in File C != USSGL 4901 + 490110 + 4931 + 4981 in File C for the same
        date context and TAS/DEFC/PYA combination (CPE)
    """

    af = AwardFinancialFactory(obligations_delivered_orde_cpe=1, ussgl490100_delivered_orde_cpe=None,
                               ussgl493100_delivered_orde_cpe=None, ussgl498100_upward_adjustm_cpe=None,
                               ussgl490110_rein_deliv_ord_cpe=None)

    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(obligations_delivered_orde_cpe=1, ussgl490100_delivered_orde_cpe=1,
                               ussgl493100_delivered_orde_cpe=1, ussgl498100_upward_adjustm_cpe=1,
                               ussgl490110_rein_deliv_ord_cpe=1)

    assert number_of_errors(_FILE, database, models=[af]) == 1
