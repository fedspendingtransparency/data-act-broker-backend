from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd38_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "funding_office_code"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ FundingOfficeCode must be six characters long.
    """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(funding_office_code='AAAAAA')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(funding_office_code='111111')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(funding_office_code='AAA111')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(funding_office_code='')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(funding_office_code=None)
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ FundingOfficeCode must be six characters long.
    """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(funding_office_code='AAAA1')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(funding_office_code='AAAAAAA')
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 2
