from tests.unit.dataactcore.factories.staging import AwardFinancialFactory, AwardProcurementFactory
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c11_cross_file'


def test_column_headers(database):
    expected_subset = {'row_number', 'piid', 'parent_award_id'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Unique PIID, ParentAwardId from file C exists in file D1 during the same reporting period. """

    af = AwardFinancialFactory(piid='some_piid', parent_award_id='some_parent_award_id',
                               transaction_obligated_amou='12345')
    ap = AwardProcurementFactory(piid='some_piid', parent_award_id='some_parent_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 0

    af = AwardFinancialFactory(piid='some_piid', parent_award_id='some_parent_award_id',
                               transaction_obligated_amou='12345')
    ap_1 = AwardProcurementFactory(piid='some_piid', parent_award_id='some_parent_award_id')
    ap_2 = AwardProcurementFactory(piid='some_piid', parent_award_id='some_parent_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap_1, ap_2]) == 0

    af = AwardFinancialFactory(piid=None, parent_award_id='some_parent_award_id',
                               transaction_obligated_amou='12345')
    ap = AwardProcurementFactory(piid='some_piid', parent_award_id='some_parent_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 0

    af = AwardFinancialFactory(piid='some_piid', parent_award_id=None, transaction_obligated_amou='12345')
    ap = AwardProcurementFactory(piid='some_piid', parent_award_id=None)

    assert number_of_errors(_FILE, database, models=[af, ap]) == 0

    # Not perform when no transaction obligated amount value in the field
    af = AwardFinancialFactory(piid='some_piid', parent_award_id='some_parent_award_id',
                               transaction_obligated_amou=None)
    ap = AwardProcurementFactory(piid='some_other_piid', parent_award_id='some_parent_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 0

    # Not perform when no transaction obligated amount value in the field
    af = AwardFinancialFactory(piid='some_piid', parent_award_id='some_parent_award_id',
                               transaction_obligated_amou=None)
    ap = AwardProcurementFactory(piid='some_piid', parent_award_id='some_other_parent_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 0

    af = AwardFinancialFactory(piid=None, parent_award_id=None, transaction_obligated_amou='12345')
    ap = AwardProcurementFactory(piid='some_piid', parent_award_id='some_other_parent_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 0


def test_failure(database):
    """ Unique PIID, ParentAwardId from file C doesn't exist in file D1 during the same reporting period. """

    # Perform when there's a transaction obligated amount value in the field
    af = AwardFinancialFactory(piid='some_piid', parent_award_id='some_parent_award_id',
                               allocation_transfer_agency=None, transaction_obligated_amou='12345')
    ap = AwardProcurementFactory(piid='some_other_piid', parent_award_id='some_parent_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 1

    # Perform when there's a transaction obligated amount value in the field
    af = AwardFinancialFactory(piid='some_piid', parent_award_id='some_parent_award_id',
                               allocation_transfer_agency='bad', transaction_obligated_amou='12345')
    ap = AwardProcurementFactory(piid='some_piid', parent_award_id='some_other_parent_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 1

    af = AwardFinancialFactory(piid='some_piid', parent_award_id=None,
                               allocation_transfer_agency='bad', transaction_obligated_amou='12345')
    ap = AwardProcurementFactory(piid='some_other_piid', parent_award_id='some_other_parent_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 1


def test_valid_allocation_transfer_agency(database):
    """If File C (award financial) record has a valid allocation transfer agency, rule always passes."""

    cgac = CGACFactory(cgac_code='good')
    # Perform when there's a transaction obligated amount value in the field
    af = AwardFinancialFactory(piid='some_piid', parent_award_id='some_parent_award_id',
                               allocation_transfer_agency=cgac.cgac_code, transaction_obligated_amou='12345')
    ap = AwardProcurementFactory(piid='some_other_piid', parent_award_id='some_parent_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap, cgac]) == 0
