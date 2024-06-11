from datetime import date
from dateutil.relativedelta import relativedelta

from tests.unit.dataactcore.factories.domain import OfficeFactory
from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs38_2_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'funding_office_code', 'action_date', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test when provided, FundingOfficeCode must be a valid value from the Federal Hierarchy, including being
        designated specifically as a Funding Office in the hierarchy.
    """

    office_1 = OfficeFactory(office_code='12345a', contract_funding_office=True,
                             financial_assistance_funding_office=False,
                             effective_start_date='01/01/2020', effective_end_date=None)
    office_2 = OfficeFactory(office_code='12345b', contract_funding_office=False,
                             financial_assistance_funding_office=True,
                             effective_start_date='01/01/2020', effective_end_date=date.today() + relativedelta(days=1))
    fabs_1 = FABSFactory(funding_office_code='12345a', correction_delete_indicatr='', action_date=date.today())

    # test case insensitive
    fabs_2 = FABSFactory(funding_office_code='12345A', correction_delete_indicatr='c', action_date=date.today())
    fabs_3 = FABSFactory(funding_office_code='', correction_delete_indicatr=None, action_date=date.today())
    fabs_4 = FABSFactory(funding_office_code=None, correction_delete_indicatr='C', action_date=date.today())

    # Testing second type of funding office
    fabs_5 = FABSFactory(funding_office_code='12345b', correction_delete_indicatr='', action_date=date.today())

    # Ignore correction delete indicator of D
    fabs_6 = FABSFactory(funding_office_code='1234567', correction_delete_indicatr='d', action_date=date.today())

    errors = number_of_errors(_FILE, database, models=[office_1, office_2, fabs_1, fabs_2, fabs_3, fabs_4, fabs_5,
                                                       fabs_6])
    assert errors == 0


def test_failure_invalid_office(database):
    """ Test fail when provided, FundingOfficeCode must be a valid value from the Federal Hierarchy, including being
        designated specifically as a Funding Office in the hierarchy.
    """

    office_1 = OfficeFactory(office_code='123456', contract_funding_office=True,
                             financial_assistance_funding_office=True,
                             effective_start_date='01/01/2020', effective_end_date=None)
    office_2 = OfficeFactory(office_code='987654', contract_funding_office=False,
                             financial_assistance_funding_office=False,
                             effective_start_date='01/01/2020', effective_end_date=None)
    fabs_1 = FABSFactory(funding_office_code='12345', correction_delete_indicatr=None, action_date=date.today())
    fabs_2 = FABSFactory(funding_office_code='1234567', correction_delete_indicatr='', action_date=date.today())

    # Test fail if funding office is false even if code matches
    fabs_3 = FABSFactory(funding_office_code='987654', correction_delete_indicatr='c', action_date=date.today())

    errors = number_of_errors(_FILE, database, models=[office_1, office_2, fabs_1, fabs_2, fabs_3])
    assert errors == 3


def test_failure_inactive_office(database):
    """ Test fail when provided, FundingOfficeCode must be a valid value from the Federal Hierarchy, including being
        designated specifically as a Funding Office in the hierarchy.
    """

    office = OfficeFactory(office_code='12345b', contract_funding_office=False,
                           financial_assistance_funding_office=True,
                           effective_start_date='01/01/2020', effective_end_date=date.today() + relativedelta(days=1))
    # Action date too early
    fabs_1 = FABSFactory(funding_office_code='12345a', correction_delete_indicatr='',
                         action_date=date.today() - relativedelta(days=1))
    # Action date too late
    fabs_2 = FABSFactory(funding_office_code='12345a', correction_delete_indicatr='',
                         action_date=date.today() + relativedelta(months=1))

    errors = number_of_errors(_FILE, database, models=[office, fabs_1, fabs_2])
    assert errors == 2
