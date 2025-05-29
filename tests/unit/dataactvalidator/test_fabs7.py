from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs7"


def test_column_headers(database):
    expected_subset = {"row_number", "record_type", "uri", "uniqueid_AssistanceTransactionUniqueKey"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Tests URI is a required field for aggregate records (i.e., when RecordType = 1)."""
    fabs_1 = FABSFactory(record_type=1, uri="something", correction_delete_indicatr="")
    fabs_2 = FABSFactory(record_type=2, uri=None, correction_delete_indicatr="c")
    fabs_3 = FABSFactory(record_type=3, uri="", correction_delete_indicatr=None)

    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(record_type=1, uri="", correction_delete_indicatr="d")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """Tests URI is not required field for non-aggregate records (i.e., when RecordType != 1)."""
    fabs_1 = FABSFactory(record_type=1, uri=None, correction_delete_indicatr="")
    fabs_2 = FABSFactory(record_type=1, uri="", correction_delete_indicatr="C")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2])
    assert errors == 2
