from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
import pytest

_FILE = 'b12_award_financial_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'by_direct_reimbursable_fun'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


@pytest.mark.parametrize('af', [AwardFinancialFactory(by_direct_reimbursable_fun=None),
                                AwardFinancialFactory(by_direct_reimbursable_fun=''),
                                AwardFinancialFactory(by_direct_reimbursable_fun='R'),
                                AwardFinancialFactory(by_direct_reimbursable_fun='r'),
                                AwardFinancialFactory(by_direct_reimbursable_fun='D'),
                                AwardFinancialFactory(by_direct_reimbursable_fun='d')])
def test_success(database, af):
    """ Test by_direct_reimbursable_fun is '', R, or D """
    assert number_of_errors(_FILE, database, models=[af]) == 0


def test_failure(database):
    """ Test by_direct_reimbursable_fun is not '', R, or D """

    af = AwardFinancialFactory(by_direct_reimbursable_fun='x')
    assert number_of_errors(_FILE, database, models=[af]) == 1