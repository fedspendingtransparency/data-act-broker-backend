from tests.unit.dataactcore.factories.staging import AwardFinancialFactory, AwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from random import randint

_FILE = 'c9_award_financial'

def test_column_headers(database):
    expected_subset = {'row_number', "fain", "uri"}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test case where all fain and uri in D2 are present in C """

    # Test with only one of fain/uri
    award_fin_fain = AwardFinancialFactory(uri = None)
    award_fin_uri = AwardFinancialFactory(fain = None)
    afa_fain = AwardFinancialAssistanceFactory(fain = award_fin_fain.fain, uri = None, federal_action_obligation = randint(1,1000))
    afa_uri = AwardFinancialAssistanceFactory(fain = None, uri = award_fin_uri.uri, federal_action_obligation = randint(1,1000))
    # Test with both fain and uri
    award_fin = AwardFinancialFactory()
    afa = AwardFinancialAssistanceFactory(fain = award_fin.fain, uri = award_fin.uri, federal_action_obligation = randint(1,1000))
    # Test with zero federal action obligation
    afa_zero = AwardFinancialAssistanceFactory(federal_action_obligation = 0)

    assert number_of_errors(_FILE, database, models=[award_fin_fain, award_fin_uri, afa_fain, afa_uri, afa_zero,
                                                     award_fin, afa]) == 0

def test_failure(database):
    """ Test fain and uri present in D2 but not in C """
    award_fin = AwardFinancialFactory(fain = None, uri = None)
    afa_fain = AwardFinancialAssistanceFactory(uri = None, federal_action_obligation = randint(1,1000))
    afa_uri = AwardFinancialAssistanceFactory(fain = None, federal_action_obligation = randint(1,1000))
    # Test with both fain and uri
    afa = AwardFinancialAssistanceFactory(federal_action_obligation = randint(1,1000))

    assert number_of_errors(_FILE, database, models=[award_fin, afa_fain, afa_uri, afa]) == 3
