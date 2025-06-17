from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabsreq1"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "award_description",
        "correction_delete_indicatr",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test AwardDescription is required for all submissions except delete records."""

    fabs = FABSFactory(correction_delete_indicatr="C", award_description="Yes")
    fabs_2 = FABSFactory(correction_delete_indicatr="", award_description="No")

    # Test ignoring for D records
    fabs_3 = FABSFactory(correction_delete_indicatr="d", award_description=None)
    fabs_4 = FABSFactory(correction_delete_indicatr="D", award_description="")
    fabs_5 = FABSFactory(correction_delete_indicatr="D", award_description="Yes")

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 0


def test_failure(database):
    """Test fail AwardDescription is required for all submissions except delete records."""

    fabs = FABSFactory(correction_delete_indicatr="c", award_description=None)
    fabs_2 = FABSFactory(correction_delete_indicatr=None, award_description="")

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2])
    assert errors == 2
