from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'b12_award_financial_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'by_direct_reimbursable_fun', 'uniqueid_TAS', 'uniqueid_ObjectClass'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test by_direct_reimbursable_fun is '', R, or D """
    af1 = AwardFinancialFactory(by_direct_reimbursable_fun=None)
    af2 = AwardFinancialFactory(by_direct_reimbursable_fun='')
    af3 = AwardFinancialFactory(by_direct_reimbursable_fun='R')
    af4 = AwardFinancialFactory(by_direct_reimbursable_fun='r')
    af5 = AwardFinancialFactory(by_direct_reimbursable_fun='D')
    af6 = AwardFinancialFactory(by_direct_reimbursable_fun='d')
    assert number_of_errors(_FILE, database, models=[af1, af2, af3, af4, af5, af6]) == 0


def test_failure(database):
    """ Test by_direct_reimbursable_fun is not '', R, or D """

    af = AwardFinancialFactory(by_direct_reimbursable_fun='x')
    assert number_of_errors(_FILE, database, models=[af]) == 1
