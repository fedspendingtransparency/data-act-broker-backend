from tests.unit.dataactcore.factories.domain import FundingOpportunityFactory
from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs47_4'


def test_column_headers(database):
    expected_subset = {'row_number', 'funding_opportunity_number', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test when provided, FundingOpportunityNumber should match a FundingOpportunityNumber within an existing notice
        of funding opportunity on Grants.gov.
    """
    fon = FundingOpportunityFactory(funding_opportunity_number='123-abC')
    # test that case is irrelevant
    fabs_1 = FABSFactory(funding_opportunity_number='123-ABC', correction_delete_indicatr='C')
    fabs_2 = FABSFactory(funding_opportunity_number='123-Abc', correction_delete_indicatr=None)

    # Ignored for CorrectionDeleteIndicator of D
    fabs_3 = FABSFactory(funding_opportunity_number='123', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fon, fabs_1, fabs_2, fabs_3])
    assert errors == 0


def test_failure(database):
    """ Test failure when provided, FundingOpportunityNumber should match a FundingOpportunityNumber within an existing
        notice of funding opportunity on Grants.gov.
    """
    fon = FundingOpportunityFactory(funding_opportunity_number='123-abC')
    fabs_1 = FABSFactory(funding_opportunity_number='123', correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[fon, fabs_1])
    assert errors == 1
