from tests.unit.dataactcore.factories.staging import AwardFinancialFactory, AwardProcurementFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c11_cross_file'


def test_column_headers(database):
    expected_subset = {'row_number', 'piid', 'parent_award_id'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Unique PIID, or combination of PIID/ParentAwardId, from file C exists in file D1 during the same reporting
        period. Do not process if allocation transfer agency is not null and does not match agency ID.
    """

    af = AwardFinancialFactory(piid='some_pIId', parent_award_id='some_PARent_award_id',
                               allocation_transfer_agency=None, transaction_obligated_amou='12345')
    ap = AwardProcurementFactory(piid='some_piid', parent_award_id='some_parent_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 0

    af = AwardFinancialFactory(piid='some_pIId', parent_award_id='some_PARent_award_id',
                               allocation_transfer_agency=None, transaction_obligated_amou='12345')
    ap_1 = AwardProcurementFactory(piid='some_piid', parent_award_id='some_parent_award_id')
    ap_2 = AwardProcurementFactory(piid='some_piid', parent_award_id='some_pareNT_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap_1, ap_2]) == 0

    af = AwardFinancialFactory(piid=None, parent_award_id='some_parENt_award_id',
                               allocation_transfer_agency=None, transaction_obligated_amou='12345')
    ap = AwardProcurementFactory(piid='some_piid', parent_award_id='some_PArent_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 0

    af = AwardFinancialFactory(piid='some_pIId', parent_award_id=None, allocation_transfer_agency=None,
                               transaction_obligated_amou='12345')
    ap = AwardProcurementFactory(piid='some_piid', parent_award_id=None)

    assert number_of_errors(_FILE, database, models=[af, ap]) == 0

    # Not perform when no transaction obligated amount value in the field
    af = AwardFinancialFactory(piid='some_pIId', parent_award_id='some_PARent_award_id',
                               allocation_transfer_agency=None, transaction_obligated_amou=None)
    ap = AwardProcurementFactory(piid='some_other_piid', parent_award_id='some_pareNT_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 0

    # Not perform when no transaction obligated amount value in the field
    af = AwardFinancialFactory(piid='some_piid', parent_award_id='some_parEnt_award_id',
                               allocation_transfer_agency=None, transaction_obligated_amou=None)
    ap = AwardProcurementFactory(piid='some_pIId', parent_award_id='some_other_parent_Award_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 0

    af = AwardFinancialFactory(piid=None, parent_award_id=None, allocation_transfer_agency=None,
                               transaction_obligated_amou='12345')
    ap = AwardProcurementFactory(piid='some_pIId', parent_award_id='some_other_parent_awARd_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 0

    # Not perform when allocation transfer agency is filled in but is different from agency id
    af = AwardFinancialFactory(piid='some_piid', parent_award_id='some_paREnt_award_id',
                               allocation_transfer_agency='good', agency_identifier='red',
                               transaction_obligated_amou='12345')
    ap = AwardProcurementFactory(piid='some_pIId', parent_award_id='some_Other_parent_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 0


def test_failure(database):
    """ Test failure for unique PIID, or combination of PIID/ParentAwardId, from file C exists in file D1 during the
        same reporting period. Do not process if allocation transfer agency is not null and does not match agency ID.
    """

    # Perform when there's a transaction obligated amount value in the field
    af = AwardFinancialFactory(piid='some_pIId', parent_award_id='some_pARent_award_id',
                               allocation_transfer_agency=None, transaction_obligated_amou='12345')
    ap = AwardProcurementFactory(piid='some_other_piid', parent_award_id='some_parent_AWard_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 1

    # Perform when there's an ata in the field and it matches the aid
    af = AwardFinancialFactory(piid='some_piid', parent_award_id='some_parent_AWard_id',
                               allocation_transfer_agency='bad', agency_identifier='bad',
                               transaction_obligated_amou='12345')
    ap = AwardProcurementFactory(piid='some_pIId', parent_award_id='soME_other_parent_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 1

    af = AwardFinancialFactory(piid='some_piid', parent_award_id=None,
                               allocation_transfer_agency='bad', agency_identifier='bad',
                               transaction_obligated_amou='12345')
    ap = AwardProcurementFactory(piid='some_other_piid', parent_award_id='some_oTHer_parent_award_id')

    assert number_of_errors(_FILE, database, models=[af, ap]) == 1
