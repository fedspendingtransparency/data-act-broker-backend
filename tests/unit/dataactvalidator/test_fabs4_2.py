from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs4_2"


def test_column_headers(database):
    expected_subset = {"row_number", "action_date", "uniqueid_AssistanceTransactionUniqueKey"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Tests if value of action date is between 19991001 and 20991231 (a date between 10/01/1999 and 12/31/2099)."""
    fabs_1 = FABSFactory(action_date="20120725", correction_delete_indicatr="")
    fabs_2 = FABSFactory(action_date=None, correction_delete_indicatr="C")
    # Ignore if not a valid date, different rule covers this
    fabs_3 = FABSFactory(action_date="5", correction_delete_indicatr=None)
    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(action_date="19990131", correction_delete_indicatr="d")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """Tests if value of action date is not between 19991001 and 20991231 (i.e., a date between 10/01/1999 and
    12/31/2099).
    """
    fabs_1 = FABSFactory(action_date="19990131", correction_delete_indicatr="c")
    fabs_2 = FABSFactory(action_date="21000101", correction_delete_indicatr=None)

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2])
    assert errors == 2
