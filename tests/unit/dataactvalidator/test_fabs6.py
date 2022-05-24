from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs6'


def test_column_headers(database):
    expected_subset = {'row_number', 'record_type', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests success for when Record type is required and cannot be blank. It must be 1, 2, or 3 """
    fabs_1 = FABSFactory(record_type=1, correction_delete_indicatr=None)
    fabs_2 = FABSFactory(record_type=2, correction_delete_indicatr='')
    fabs_3 = FABSFactory(record_type=3, correction_delete_indicatr='c')

    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(record_type=0, correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """ Tests failure for when Record type is required and cannot be blank. It must be 1, 2, or 3 """
    fabs_1 = FABSFactory(record_type=0, correction_delete_indicatr=None)
    fabs_2 = FABSFactory(record_type=None, correction_delete_indicatr='c')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2])
    assert errors == 2
