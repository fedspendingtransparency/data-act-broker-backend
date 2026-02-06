from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs26_2"


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
    """FederalActionObligation is required for non-loans (i.e., when AssistanceType is not 07, 08, F003, or F004)."""

    fabs = FABSFactory(assistance_type="02", federal_action_obligation=0, correction_delete_indicatr="")
    fabs_2 = FABSFactory(assistance_type="10", federal_action_obligation=20, correction_delete_indicatr="c")
    fabs_3 = FABSFactory(assistance_type="F001", federal_action_obligation=5, correction_delete_indicatr="c")
    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(assistance_type="03", federal_action_obligation=None, correction_delete_indicatr="d")

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """FederalActionObligation is required for non-loans (i.e., when AssistanceType is not 07, 08, F003, or F004)."""

    fabs = FABSFactory(assistance_type="03", federal_action_obligation=None, correction_delete_indicatr="C")

    errors = number_of_errors(_FILE, database, models=[fabs])
    assert errors == 1
