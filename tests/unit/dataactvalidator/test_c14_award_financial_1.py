from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'c14_award_financial_1'

def test_column_headers(database):
    expected_subset = {'row_number', "fain", "uri", "piid"}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test cases with different combinations of fain, uri, and piid """

    # Test with only one present
    award_fin_fain = AwardFinancialFactory(uri = None, piid = None)
    award_fin_uri = AwardFinancialFactory(fain = None, piid = None)
    award_fin_piid = AwardFinancialFactory(fain = None, uri = None)
    # Test with all three
    award_fin = AwardFinancialFactory()
    # Test with one missing
    award_fin_no_fain = AwardFinancialFactory(fain = None)
    award_fin_no_uri = AwardFinancialFactory(uri = None)
    award_fin_no_piid = AwardFinancialFactory(piid = None)

    assert number_of_errors(_FILE, database, models=[award_fin_fain, award_fin_uri, award_fin_piid, award_fin,
                                                     award_fin_no_fain, award_fin_no_uri, award_fin_no_piid]) == 0

def test_failure(database):
    """ Test with fain, uri, and piid all absent """
    award_fin = AwardFinancialFactory(fain = None, uri = None, piid = None)

    assert number_of_errors(_FILE, database, models=[award_fin]) == 1
