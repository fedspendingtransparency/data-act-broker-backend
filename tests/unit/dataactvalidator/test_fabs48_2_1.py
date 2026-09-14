from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs48_2_1"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "funding_opportunity_goals",
        "assistance_type",
        "award_recipient_basis_code",
        "action_date",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test FundingOpportunityGoalsText is required for all competitive, discretionary grants and cooperative agreements
    (AssistanceType = F001 or F002 and AwardRecipientBasisCode = R01). When ActionDate is prior to October 01, 2026,
    'Not Applicable' will be accepted for not competitive and discretionary grants and cooperative agreements
    (AssistanceType = F001 or F002 and AwardRecipientBasisCode is blank).
    """
    fabs_1 = FABSFactory(funding_opportunity_goals="NOT AppLICABLE", action_date="10/01/2025", award_recipient_basis_code='', assistance_type="F002", correction_delete_indicatr="C")
    fabs_2 = FABSFactory(funding_opportunity_goals="1234", action_date="10/01/2025", award_recipient_basis_code=None, assistance_type="f001", correction_delete_indicatr="C")

    # Ignored for other assistance types
    fabs_3 = FABSFactory(funding_opportunity_goals="", action_date="10/01/2025", award_recipient_basis_code='', assistance_type="F006", correction_delete_indicatr="C")

    # Ignored for other award recipient basis codes
    fabs_4 = FABSFactory(funding_opportunity_goals="", action_date="10/01/2025", award_recipient_basis_code='R02',
                         assistance_type="F001", correction_delete_indicatr=None)

    # Ignored for Action date of 10/01/2026 or later
    fabs_5 = FABSFactory(funding_opportunity_goals=None, action_date="10/01/2026", award_recipient_basis_code=None,
                         assistance_type="F001", correction_delete_indicatr=None)

    # Ignored for CorrectionDeleteIndicator of D
    fabs_6 = FABSFactory(funding_opportunity_goals=None, action_date="10/01/2025", award_recipient_basis_code='', assistance_type="F002", correction_delete_indicatr="d")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6])
    assert errors == 0


def test_failure(database):
    """Test failure FundingOpportunityGoalsText is required for all competitive, discretionary grants and cooperative
    agreements (AssistanceType = F001 or F002 and AwardRecipientBasisCode = R01). When ActionDate is prior to
    October 01, 2026, 'Not Applicable' will be accepted for not competitive and discretionary grants and cooperative
    agreements (AssistanceType = F001 or F002 and AwardRecipientBasisCode is blank).
    """
    fabs_1 = FABSFactory(funding_opportunity_goals="", action_date="10/01/2025", award_recipient_basis_code='', assistance_type="F001", correction_delete_indicatr="C")
    fabs_2 = FABSFactory(funding_opportunity_goals=None, action_date="10/01/2025", award_recipient_basis_code=None, assistance_type="f002", correction_delete_indicatr="C")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2])
    assert errors == 2
