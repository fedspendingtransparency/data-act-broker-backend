from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs47_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'funding_opportunity_number', 'assistance_type',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test FundingOpportunityNumber must be blank for non-grants/non-cooperative agreements
        (AssistanceType = 06, 07, 08, 09, 10, or 11).
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(funding_opportunity_number='', assistance_type='06',
                                                          correction_delete_indicatr='C')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(funding_opportunity_number=None, assistance_type='09',
                                                          correction_delete_indicatr=None)

    # Ignored for other assistance types
    det_award_3 = DetachedAwardFinancialAssistanceFactory(funding_opportunity_number='123', assistance_type='03',
                                                          correction_delete_indicatr='C')

    # Ignored for CorrectionDeleteIndicator of D
    det_award_4 = DetachedAwardFinancialAssistanceFactory(funding_opportunity_number='123', assistance_type='08',
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ Test failure FundingOpportunityNumber must be blank for non-grants/non-cooperative agreements
        (AssistanceType = 06, 07, 08, 09, 10, or 11).
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(funding_opportunity_number='123', assistance_type='06',
                                                          correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[det_award_1])
    assert errors == 1
