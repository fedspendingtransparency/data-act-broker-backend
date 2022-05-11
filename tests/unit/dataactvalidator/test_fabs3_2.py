from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs3_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'action_type', 'record_type', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests if ActionType is one of the following values: “A”, “B”, “C”, “D”, or "E". """
    fabs_1 = FABSFactory(action_type='a', correction_delete_indicatr='')
    fabs_2 = FABSFactory(action_type='B', correction_delete_indicatr=None)
    fabs_3 = FABSFactory(action_type='c', correction_delete_indicatr='c')
    fabs_4 = FABSFactory(action_type='D', correction_delete_indicatr='C')
    fabs_5 = FABSFactory(action_type='e', correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    fabs_6 = FABSFactory(action_type='Thing', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6])
    assert errors == 0


def test_failure(database):
    """ Tests if ActionType is not one of the following values: “A”, “B”, “C”, “D”, or "E". """
    fabs_1 = FABSFactory(action_type='random', correction_delete_indicatr='c')
    fabs_2 = FABSFactory(action_type='', correction_delete_indicatr='')
    fabs_3 = FABSFactory(action_type=None, correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3])
    assert errors == 3
