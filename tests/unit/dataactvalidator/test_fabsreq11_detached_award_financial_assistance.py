from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabsreq11_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'action_type', 'correction_delete_indicatr',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test ActionType is required for all submissions except delete records. """

    fabs = FABSFactory(correction_delete_indicatr='C', action_type='c')
    fabs_2 = FABSFactory(correction_delete_indicatr='', action_type='A')

    # Test ignoring for D records
    fabs_3 = FABSFactory(correction_delete_indicatr='d', action_type=None)
    fabs_4 = FABSFactory(correction_delete_indicatr='D', action_type='')
    fabs_5 = FABSFactory(correction_delete_indicatr='d', action_type='Name')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 0


def test_failure(database):
    """ Test fail ActionType is required for all submissions except delete records. """

    fabs = FABSFactory(correction_delete_indicatr='c', action_type=None)
    fabs_2 = FABSFactory(correction_delete_indicatr=None, action_type='')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2])
    assert errors == 2
