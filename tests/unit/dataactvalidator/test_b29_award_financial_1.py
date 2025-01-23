from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b29_award_financial_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'prior_year_adjustment', 'uniqueid_TAS'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ PYA must be X, B, P, or Blank """

    afs =[
        AwardFinancialFactory(prior_year_adjustment='X'),
        AwardFinancialFactory(prior_year_adjustment='x'),
        AwardFinancialFactory(prior_year_adjustment='B'),
        AwardFinancialFactory(prior_year_adjustment='b'),
        AwardFinancialFactory(prior_year_adjustment='P'),
        AwardFinancialFactory(prior_year_adjustment='p'),
        AwardFinancialFactory(prior_year_adjustment=''),
        AwardFinancialFactory(prior_year_adjustment=None),
    ]
    assert number_of_errors(_FILE, database, models=afs) == 0


def test_failure(database):
    """ Tests failure if PYA is not X, B, P, or Blank """

    afs = [
        AwardFinancialFactory(prior_year_adjustment='Fail'),
        AwardFinancialFactory(prior_year_adjustment=0),
        AwardFinancialFactory(prior_year_adjustment='None'),
        AwardFinancialFactory(prior_year_adjustment='A'),
    ]
    assert number_of_errors(_FILE, database, models=afs) == 4
