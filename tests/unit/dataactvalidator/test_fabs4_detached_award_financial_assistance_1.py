from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs4_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'action_date', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests success for Action date in YYYYMMDD format. """
    fabs_1 = FABSFactory(action_date='19990131', correction_delete_indicatr='')

    # Ignore correction delete indicator of D
    fabs_2 = FABSFactory(action_date='12345678', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2])
    assert errors == 0


def test_failure(database):
    """ Tests failure for Action date in YYYYMMDD format. """
    fabs_1 = FABSFactory(action_date='19990132', correction_delete_indicatr='')
    fabs_2 = FABSFactory(action_date='19991331', correction_delete_indicatr=None)
    fabs_3 = FABSFactory(action_date=None, correction_delete_indicatr='c')
    fabs_4 = FABSFactory(action_date="", correction_delete_indicatr='C')
    fabs_5 = FABSFactory(action_date='200912', correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 5
