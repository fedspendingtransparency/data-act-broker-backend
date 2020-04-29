from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'c14_award_financial_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'fain', 'uri', 'piid', 'uniqueid_TAS', 'uniqueid_PIID', 'uniqueid_FAIN',
                       'uniqueid_URI'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test cases with different combinations of fain, uri, and piid """

    # Test with only one present
    award_fin_fain = AwardFinancialFactory(uri=None, piid=None)
    award_fin_uri = AwardFinancialFactory(fain=None, piid=None)
    award_fin_piid = AwardFinancialFactory(fain=None, uri=None)

    assert number_of_errors(_FILE, database,
                            models=[award_fin_fain, award_fin_uri, award_fin_piid]) == 0


def test_failure(database):
    """ Test with fain, uri, and piid all present """
    # Test with all three
    award_fin = AwardFinancialFactory()
    # Test with any 2 present
    award_fin_piid_uri = AwardFinancialFactory(fain=None)
    award_fin_piid_fain = AwardFinancialFactory(uri=None)
    award_fin_fain_uri = AwardFinancialFactory(piid=None)

    assert number_of_errors(_FILE, database, models=[award_fin, award_fin_piid_uri, award_fin_piid_fain,
                                                     award_fin_fain_uri]) == 4
