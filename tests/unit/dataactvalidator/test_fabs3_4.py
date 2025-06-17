from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs3_4"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "action_type",
        "record_type",
        "correction_delete_indicatr",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ActionType = E is only valid for mixed aggregate records (RecordType = 1)."""
    fabs_1 = FABSFactory(record_type=1, action_type="E", correction_delete_indicatr=None)
    fabs_2 = FABSFactory(record_type=1, action_type="e", correction_delete_indicatr="C")

    # Ignore delete record
    fabs_3 = FABSFactory(record_type=2, action_type="e", correction_delete_indicatr="D")

    # Can have whatever we want for other record types for this rule
    fabs_4 = FABSFactory(record_type=2, action_type="a", correction_delete_indicatr="")

    # Can have whatever other action type we want for record type 1
    fabs_5 = FABSFactory(record_type=1, action_type="a", correction_delete_indicatr="")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 0


def test_failure(database):
    """Fail ActionType = E is only valid for mixed aggregate records (RecordType = 1)."""

    fabs_1 = FABSFactory(record_type=2, action_type="e", correction_delete_indicatr="c")

    errors = number_of_errors(_FILE, database, models=[fabs_1])
    assert errors == 1
