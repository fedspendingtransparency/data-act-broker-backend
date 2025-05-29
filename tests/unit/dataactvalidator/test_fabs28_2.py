from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs28_2"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "assistance_type",
        "face_value_loan_guarantee",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """FaceValueOfDirectLoanOrLoanGuarantee must be blank or 0 for non-loans
    (i.e., when AssistanceType is not 07 or 08).
    """

    fabs = FABSFactory(assistance_type="03", face_value_loan_guarantee=None, correction_delete_indicatr="")
    fabs_2 = FABSFactory(assistance_type="10", face_value_loan_guarantee=None, correction_delete_indicatr="c")
    fabs_3 = FABSFactory(assistance_type="03", face_value_loan_guarantee=0, correction_delete_indicatr=None)
    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(assistance_type="11", face_value_loan_guarantee=20, correction_delete_indicatr="d")

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """FaceValueOfDirectLoanOrLoanGuarantee must be blank or 0 for non-loans
    (i.e., when AssistanceType is not 07 or 08).
    """

    fabs = FABSFactory(assistance_type="11", face_value_loan_guarantee=20, correction_delete_indicatr="")

    errors = number_of_errors(_FILE, database, models=[fabs])
    assert errors == 1
