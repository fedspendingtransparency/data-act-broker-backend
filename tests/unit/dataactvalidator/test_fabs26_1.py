from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs26_1"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "assistance_type",
        "federal_action_obligation",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """FederalActionObligation must be blank for loans (i.e., when AssistanceType = 07 or 08)."""

    fabs = FABSFactory(assistance_type="07", federal_action_obligation=None, correction_delete_indicatr="")
    fabs_2 = FABSFactory(assistance_type="08", federal_action_obligation=None, correction_delete_indicatr="c")
    fabs_3 = FABSFactory(assistance_type="07", federal_action_obligation=0, correction_delete_indicatr=None)
    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(assistance_type="08", federal_action_obligation=20, correction_delete_indicatr="d")

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """FederalActionObligation must be blank for loans (i.e., when AssistanceType = 07 or 08)."""

    fabs = FABSFactory(assistance_type="08", federal_action_obligation=20, correction_delete_indicatr="")

    errors = number_of_errors(_FILE, database, models=[fabs])
    assert errors == 1
