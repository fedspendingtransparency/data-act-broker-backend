from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabsreq6'


def test_column_headers(database):
    expected_subset = {'row_number', 'business_types', 'correction_delete_indicatr',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test BusinessTypes is required for all submissions except delete records. """

    fabs = FABSFactory(correction_delete_indicatr='C', business_types='ABC')
    fabs_2 = FABSFactory(correction_delete_indicatr='', business_types='abc')

    # Test ignoring for D records
    fabs_3 = FABSFactory(correction_delete_indicatr='d', business_types=None)
    fabs_4 = FABSFactory(correction_delete_indicatr='D', business_types='')
    fabs_5 = FABSFactory(correction_delete_indicatr='D', business_types='ABC')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 0


def test_failure(database):
    """ Test fail BusinessTypes is required for all submissions except delete records. """

    fabs = FABSFactory(correction_delete_indicatr='c', business_types=None)
    fabs_2 = FABSFactory(correction_delete_indicatr=None, business_types='')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2])
    assert errors == 2
