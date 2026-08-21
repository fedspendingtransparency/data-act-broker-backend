from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs5_4"


def test_column_headers(database):
    expected_subset = {"row_number", "award_recipient_basis_code", "uniqueid_AssistanceTransactionUniqueKey"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test success when provided, AwardRecipientBasisCode must contain one of the values between "R01" and "R04" """
    fabs_1 = FABSFactory(award_recipient_basis_code="r01", correction_delete_indicatr="")

    # Ignore blank
    fabs_2 = FABSFactory(award_recipient_basis_code="", correction_delete_indicatr="")
    fabs_3 = FABSFactory(award_recipient_basis_code=None, correction_delete_indicatr="")

    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(award_recipient_basis_code="ABC", correction_delete_indicatr="d")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """Tests failure when provided, AwardRecipientBasisCode must contain one of the values between "R01" and "R04" """
    fabs_1 = FABSFactory(award_recipient_basis_code="ABC")

    errors = number_of_errors(_FILE, database, models=[fabs_1])
    assert errors == 1
