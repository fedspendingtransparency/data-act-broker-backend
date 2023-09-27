from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs47_3'


def test_column_headers(database):
    expected_subset = {'row_number', 'funding_opportunity_number', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test when provided, FundingOpportunityNumber must only contain letters (a-z, lowercase or uppercase),
        numerals (0-9), or the ‘-‘ character, to ensure consistency with Grants.gov FundingOpportunityNumber
        formatting requirements.
    """
    fabs_1 = FABSFactory(funding_opportunity_number='abE-223', correction_delete_indicatr='C')
    fabs_2 = FABSFactory(funding_opportunity_number='1', correction_delete_indicatr=None)

    # Still works for blanks
    fabs_3 = FABSFactory(funding_opportunity_number=None, correction_delete_indicatr='C')
    fabs_4 = FABSFactory(funding_opportunity_number='', correction_delete_indicatr='')

    # Ignored for CorrectionDeleteIndicator of D
    fabs_5 = FABSFactory(funding_opportunity_number='()', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 0


def test_failure(database):
    """ Test failure when provided, FundingOpportunityNumber must only contain letters (a-z, lowercase or uppercase),
        numerals (0-9), or the ‘-‘ character, to ensure consistency with Grants.gov FundingOpportunityNumber
        formatting requirements.
    """
    fabs_1 = FABSFactory(funding_opportunity_number='123-()', assistance_type='06', correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[fabs_1])
    assert errors == 1
