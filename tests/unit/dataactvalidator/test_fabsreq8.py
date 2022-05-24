from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabsreq8'


def test_column_headers(database):
    expected_subset = {'row_number', 'action_date', 'correction_delete_indicatr',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test ActionDate is required for all submissions except delete records. """

    fabs = FABSFactory(correction_delete_indicatr='C', action_date='20171001')
    fabs_2 = FABSFactory(correction_delete_indicatr='', action_date='20171001')

    # Test ignoring for D records
    fabs_3 = FABSFactory(correction_delete_indicatr='d', action_date=None)
    fabs_4 = FABSFactory(correction_delete_indicatr='D', action_date='')
    fabs_5 = FABSFactory(correction_delete_indicatr='D', action_date='20171001')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 0


def test_failure(database):
    """ Test fail ActionDate is required for all submissions except delete records. """

    fabs = FABSFactory(correction_delete_indicatr='c', action_date=None)
    fabs_2 = FABSFactory(correction_delete_indicatr=None, action_date='')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2])
    assert errors == 2
