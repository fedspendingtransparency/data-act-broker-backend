from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs45_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'indirect_federal_sharing', 'federal_action_obligation',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test when both are provided, IndirectCostFederalShareAmount should be less than or equal to
        FederalActionObligation.
    """

    # One or both not provided, rule ignored
    fabs_1 = FABSFactory(indirect_federal_sharing=None, federal_action_obligation=None)
    fabs_2 = FABSFactory(indirect_federal_sharing=123, federal_action_obligation=None)
    fabs_3 = FABSFactory(indirect_federal_sharing=None, federal_action_obligation=123)

    # ICFSA is 0, rule ignored
    fabs_4 = FABSFactory(indirect_federal_sharing=0, federal_action_obligation=123)

    # Both have the same sign and are appropriately valued
    fabs_5 = FABSFactory(indirect_federal_sharing=-1, federal_action_obligation=-1)
    fabs_6 = FABSFactory(indirect_federal_sharing=5, federal_action_obligation=6)

    # Ignore when CorrectionDeleteIndicator is D
    fabs_7 = FABSFactory(indirect_federal_sharing=123, federal_action_obligation=0, correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7])
    assert errors == 0


def test_failure(database):
    """ Test failure when both are provided, IndirectCostFederalShareAmount should be less than or equal to
        FederalActionObligation.
    """

    # ICFSA is not 0 but FAO is
    fabs_1 = FABSFactory(indirect_federal_sharing=123, federal_action_obligation=0)

    # Differing signs
    fabs_2 = FABSFactory(indirect_federal_sharing=-1, federal_action_obligation=1)
    fabs_3 = FABSFactory(indirect_federal_sharing=1, federal_action_obligation=-1)

    # Same sign, absolute value incorrect
    fabs_4 = FABSFactory(indirect_federal_sharing=5, federal_action_obligation=4)
    fabs_5 = FABSFactory(indirect_federal_sharing=-5, federal_action_obligation=-4)

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 5
