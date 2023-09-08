from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs47_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'funding_opportunity_number', 'assistance_type',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test FundingOpportunityNumber is required for all grants and cooperative agreements
        (AssistanceType = 02, 03, 04, or 05).
    """
    fabs_1 = FABSFactory(funding_opportunity_number='1234', assistance_type='02', correction_delete_indicatr='C')
    fabs_2 = FABSFactory(funding_opportunity_number='abcG-2', assistance_type='05', correction_delete_indicatr=None)

    # Ignored for other assistance types
    fabs_3 = FABSFactory(funding_opportunity_number='', assistance_type='08', correction_delete_indicatr='C')

    # Ignored for CorrectionDeleteIndicator of D
    fabs_4 = FABSFactory(funding_opportunity_number=None, assistance_type='04', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """ Test failure FundingOpportunityNumber is required for all grants and cooperative agreements
        (AssistanceType = 02, 03, 04, or 05).
    """
    fabs_1 = FABSFactory(funding_opportunity_number=None, assistance_type='02', correction_delete_indicatr='C')
    fabs_2 = FABSFactory(funding_opportunity_number='', assistance_type='03', correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2])
    assert errors == 2
