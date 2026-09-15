from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs48_1_2"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "funding_opportunity_goals",
        "assistance_type",
        "award_recipient_basis_code",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """FundingOpportunityGoalsText must be blank or "Not Applicable" for AssistanceType = F001 or F002 and
    AwardRecipientBasisCode = R02, R03, or R04.
    """
    fabs_1 = FABSFactory(
        funding_opportunity_goals="",
        assistance_type="f001",
        award_recipient_basis_code="r03",
        correction_delete_indicatr="C",
    )
    fabs_2 = FABSFactory(
        funding_opportunity_goals="Not applicable",
        assistance_type="F002",
        award_recipient_basis_code="R02",
        correction_delete_indicatr=None,
    )

    # Ignored for other assistance types
    fabs_3 = FABSFactory(
        funding_opportunity_goals="123",
        assistance_type="F004",
        award_recipient_basis_code="R02",
        correction_delete_indicatr="C",
    )

    # Ignored for other award recipient basis codes
    fabs_4 = FABSFactory(
        funding_opportunity_goals="123",
        assistance_type="F001",
        award_recipient_basis_code="R01",
        correction_delete_indicatr="C",
    )

    # Ignored for CorrectionDeleteIndicator of D
    fabs_5 = FABSFactory(
        funding_opportunity_goals="123",
        assistance_type="F002",
        award_recipient_basis_code="R01",
        correction_delete_indicatr="d",
    )

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 0


def test_failure(database):
    """Test failure FundingOpportunityGoalsText must be blank or "Not Applicable" for AssistanceType = F001 or F002 and
    AwardRecipientBasisCode = R02, R03, or R04.
    """
    fabs_1 = FABSFactory(
        funding_opportunity_goals="123",
        assistance_type="F001",
        award_recipient_basis_code="r02",
        correction_delete_indicatr="C",
    )
    fabs_2 = FABSFactory(
        funding_opportunity_goals="123",
        assistance_type="F002",
        award_recipient_basis_code="R03",
        correction_delete_indicatr="",
    )

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2])
    assert errors == 2
