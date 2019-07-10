from tests.unit.dataactcore.factories.domain import OfficeFactory
from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs38_detached_award_financial_assistance_2_1'


def test_column_headers(database):
    expected_subset = {"row_number", "funding_office_code"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test when provided, FundingOfficeCode must be a valid value from the Federal Hierarchy, including being
        designated specifically as a Funding Office in the hierarchy.
    """

    office_1 = OfficeFactory(office_code='12345a', contract_funding_office=True,
                             financial_assistance_funding_office=False)
    office_2 = OfficeFactory(office_code='12345b', contract_funding_office=False,
                             financial_assistance_funding_office=True)
    det_award_1 = DetachedAwardFinancialAssistanceFactory(funding_office_code='12345a', correction_delete_indicatr='')
    # test case insensitive
    det_award_2 = DetachedAwardFinancialAssistanceFactory(funding_office_code='12345A', correction_delete_indicatr='c')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(funding_office_code='', correction_delete_indicatr=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(funding_office_code=None, correction_delete_indicatr='C')
    # Testing second type of funding office
    det_award_5 = DetachedAwardFinancialAssistanceFactory(funding_office_code='12345b', correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    det_award_6 = DetachedAwardFinancialAssistanceFactory(funding_office_code='1234567', correction_delete_indicatr='d')
    errors = number_of_errors(_FILE, database, models=[office_1, office_2, det_award_1, det_award_2, det_award_3,
                                                       det_award_4, det_award_5, det_award_6])
    assert errors == 0


def test_failure(database):
    """ Test fail when provided, FundingOfficeCode must be a valid value from the Federal Hierarchy, including being
        designated specifically as a Funding Office in the hierarchy.
    """

    office_1 = OfficeFactory(office_code='123456', contract_funding_office=True,
                             financial_assistance_funding_office=True)
    office_2 = OfficeFactory(office_code='987654', contract_funding_office=False,
                             financial_assistance_funding_office=False)
    det_award_1 = DetachedAwardFinancialAssistanceFactory(funding_office_code='12345')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(funding_office_code='1234567')
    # Test fail if funding office is false even if code matches
    det_award_3 = DetachedAwardFinancialAssistanceFactory(funding_office_code='987654')
    errors = number_of_errors(_FILE, database, models=[office_1, office_2, det_award_1, det_award_2, det_award_3])
    assert errors == 3
