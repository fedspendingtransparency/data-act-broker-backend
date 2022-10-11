from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c29_award_financial'


def test_column_headers(database):
    expected_subset = {'row_number', 'general_ledger_post_date', 'uniqueid_TAS', 'uniqueid_DisasterEmergencyFundCode',
                       'uniqueid_PIID', 'uniqueid_FAIN', 'uniqueid_URI'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests the File C GeneralLedgerPostDate must be blank for non-TOA (balance) rows. """

    # Both contain data
    af1 = AwardFinancialFactory(general_ledger_post_date='2020-01-01', transaction_obligated_amou=5)

    # 0 is considered a value
    af2 = AwardFinancialFactory(general_ledger_post_date='2020-01-01', transaction_obligated_amou=0)

    # Blank
    af3 = AwardFinancialFactory(general_ledger_post_date=None, transaction_obligated_amou=None)

    assert number_of_errors(_FILE, database, models=[af1, af2, af3]) == 0


def test_failure(database):
    """ Tests failure the File C GeneralLedgerPostDate must be blank for non-TOA (balance) rows. """

    af1 = AwardFinancialFactory(general_ledger_post_date='2020-01-01', transaction_obligated_amou=None)

    assert number_of_errors(_FILE, database, models=[af1]) == 1
