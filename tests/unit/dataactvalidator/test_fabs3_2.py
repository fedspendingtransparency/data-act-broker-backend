from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs3_2"


def test_column_headers(database):
    expected_subset = {"row_number", "action_type", "record_type", "uniqueid_AssistanceTransactionUniqueKey"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Tests if ActionType is one of the following values: "A1", "A2", "B1", "C1", "C2", "C3", "C4", "D1", "E1", "EX",
    "FX", "G1"."""
    fabs1 = FABSFactory(action_type="a1", correction_delete_indicatr="")
    fabs2 = FABSFactory(action_type="a2", correction_delete_indicatr="")
    fabs3 = FABSFactory(action_type="B1", correction_delete_indicatr=None)
    fabs4 = FABSFactory(action_type="c1", correction_delete_indicatr="c")
    fabs5 = FABSFactory(action_type="c2", correction_delete_indicatr="c")
    fabs6 = FABSFactory(action_type="c3", correction_delete_indicatr="c")
    fabs7 = FABSFactory(action_type="C4", correction_delete_indicatr="c")
    fabs8 = FABSFactory(action_type="D1", correction_delete_indicatr="C")
    fabs9 = FABSFactory(action_type="e1", correction_delete_indicatr="")
    fabs10 = FABSFactory(action_type="ex", correction_delete_indicatr="")
    fabs11 = FABSFactory(action_type="Fx", correction_delete_indicatr="")
    fabs12 = FABSFactory(action_type="G1", correction_delete_indicatr="")
    # Ignore correction delete indicator of D
    fabs13 = FABSFactory(action_type="Thing", correction_delete_indicatr="d")

    errors = number_of_errors(
        _FILE,
        database,
        models=[fabs1, fabs2, fabs3, fabs4, fabs5, fabs6, fabs7, fabs8, fabs9, fabs10, fabs11, fabs12, fabs13],
    )
    assert errors == 0


def test_failure(database):
    """Tests if ActionType is not one of the following values: "A1", "A2", "B1", "C1", "C2", "C3", "C4", "D1", "E1",
    "EX", "FX", "G1"."""
    fabs1 = FABSFactory(action_type="random", correction_delete_indicatr="c")
    fabs2 = FABSFactory(action_type="", correction_delete_indicatr="")
    fabs3 = FABSFactory(action_type=None, correction_delete_indicatr="C")
    fabs4 = FABSFactory(action_type="A", correction_delete_indicatr=None)

    errors = number_of_errors(_FILE, database, models=[fabs1, fabs2, fabs3, fabs4])
    assert errors == 4
