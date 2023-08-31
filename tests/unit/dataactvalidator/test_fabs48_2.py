from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs48_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'funding_opportunity_goals', 'assistance_type',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test FundingOpportunityGoalsText is required for all grants and cooperative agreements
        (AssistanceType = 02, 03, 04, or 05).
    """
    fabs_1 = FABSFactory(funding_opportunity_goals='1234', assistance_type='02', correction_delete_indicatr='C')

    # Ignored for other assistance types
    fabs_2 = FABSFactory(funding_opportunity_goals='', assistance_type='06', correction_delete_indicatr='C')

    # Ignored for CorrectionDeleteIndicator of D
    fabs_3 = FABSFactory(funding_opportunity_goals=None, assistance_type='02', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3])
    assert errors == 0


def test_failure(database):
    """ Test failure FundingOpportunityGoalsText is required for all grants and cooperative agreements
        (AssistanceType = 02, 03, 04, or 05).
    """
    fabs_1 = FABSFactory(funding_opportunity_goals='', assistance_type='04', correction_delete_indicatr='C')
    fabs_2 = FABSFactory(funding_opportunity_goals=None, assistance_type='05', correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2])
    assert errors == 2
