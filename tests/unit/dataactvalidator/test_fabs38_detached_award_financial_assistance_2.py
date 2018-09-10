from tests.unit.dataactcore.factories.domain import OfficeFactory
from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs38_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "funding_office_code"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test when provided, FundingOfficeCode must be a valid value from the Federal Hierarchy. """

    office = OfficeFactory(office_code='123456')
    det_award_1 = DetachedAwardFinancialAssistanceFactory(funding_office_code='123456')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(funding_office_code='')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(funding_office_code=None)
    errors = number_of_errors(_FILE, database, models=[office, det_award_1, det_award_2, det_award_3])
    assert errors == 0


def test_failure(database):
    """ Test fail when provided, FundingOfficeCode must be a valid value from the Federal Hierarchy. """

    office = OfficeFactory(office_code='123456')
    det_award_1 = DetachedAwardFinancialAssistanceFactory(funding_office_code='12345')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(funding_office_code='1234567')
    errors = number_of_errors(_FILE, database, models=[office, det_award_1, det_award_2])
    assert errors == 2
