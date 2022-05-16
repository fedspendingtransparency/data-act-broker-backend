from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabsreq3'


def test_column_headers(database):
    expected_subset = {'row_number', 'record_type', 'correction_delete_indicatr',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test RecordType is required for all submissions except delete records. """

    fabs = FABSFactory(correction_delete_indicatr='C', record_type=1)
    fabs_2 = FABSFactory(correction_delete_indicatr='', record_type=2)

    # Test ignoring for D records
    fabs_3 = FABSFactory(correction_delete_indicatr='d', record_type=None)
    fabs_4 = FABSFactory(correction_delete_indicatr='D', record_type=None)
    fabs_5 = FABSFactory(correction_delete_indicatr='D', record_type=1)

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 0


def test_failure(database):
    """ Test fail RecordType is required for all submissions except delete records. """

    fabs = FABSFactory(correction_delete_indicatr='c', record_type=None)
    fabs_2 = FABSFactory(correction_delete_indicatr=None, record_type=None)

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2])
    assert errors == 2
