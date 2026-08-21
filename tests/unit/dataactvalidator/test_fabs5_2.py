from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs5_2"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "assistance_type",
        "action_date",
        "award_amount_basis_code",
        "award_recipient_basis_code",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test success when AssistanceType is a Grant or Cooperative Agreement (AssistanceType = F001 or F002) with an
    ActionDate on or after October 01, 2026, AwardAmountBasisCode and AwardRecipientBasisCode must be populated.
    """
    fabs_1 = FABSFactory(
        assistance_type="F001",
        action_date="2026/10/02",
        award_amount_basis_code="A",
        award_recipient_basis_code="B",
        correction_delete_indicatr="",
    )
    fabs_2 = FABSFactory(
        assistance_type="F002",
        action_date="2026/10/02",
        award_amount_basis_code="A",
        award_recipient_basis_code="B",
        correction_delete_indicatr="C",
    )

    # Ignore earlier date
    fabs_3 = FABSFactory(
        assistance_type="F001",
        action_date="2025/10/02",
        award_amount_basis_code="",
        award_recipient_basis_code=None,
        correction_delete_indicatr="",
    )

    # Ignore different assistance type
    fabs_4 = FABSFactory(
        assistance_type="F004",
        action_date="2026/10/02",
        award_amount_basis_code=None,
        award_recipient_basis_code="",
        correction_delete_indicatr="C",
    )

    # Ignore correction delete indicator of D
    fabs_5 = FABSFactory(
        assistance_type="F001",
        action_date="2026/10/02",
        award_amount_basis_code=None,
        award_recipient_basis_code="",
        correction_delete_indicatr="d",
    )

    errors = number_of_errors(
        _FILE,
        database,
        models=[
            fabs_1,
            fabs_2,
            fabs_3,
            fabs_4,
            fabs_5,
        ],
    )
    assert errors == 0


def test_failure(database):
    """Tests failure when AssistanceType is a Grant or Cooperative Agreement (AssistanceType = F001 or F002) with an
    ActionDate on or after October 01, 2026, AwardAmountBasisCode and AwardRecipientBasisCode must be populated.
    """
    fabs_1 = FABSFactory(
        assistance_type="F002",
        action_date="2026/10/02",
        award_amount_basis_code="",
        award_recipient_basis_code="B",
        correction_delete_indicatr="C",
    )
    fabs_2 = FABSFactory(
        assistance_type="F001",
        action_date="2026/10/02",
        award_amount_basis_code="A",
        award_recipient_basis_code=None,
        correction_delete_indicatr="C",
    )
    fabs_3 = FABSFactory(
        assistance_type="F002",
        action_date="2026/10/02",
        award_amount_basis_code=None,
        award_recipient_basis_code="",
        correction_delete_indicatr="C",
    )

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3])
    assert errors == 3
