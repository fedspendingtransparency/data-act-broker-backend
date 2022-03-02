from tests.unit.dataactcore.factories.domain import SF133Factory
from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a36_appropriations'
_TAS = 'a36_appropriations_tas'


def test_column_headers(database):
    expected_subset = {'uniqueid_TAS', 'row_number', 'budget_authority_unobligat_fyb',
                       'expected_value_GTAS SF133 Line 1000', 'difference'}
    actual = set(query_columns(_FILE, database))
    assert actual == expected_subset


def test_success(database):
    """ Tests that if the SF-133 amount for line 1000 for the same fiscal year and period is populated, then
        Appropriation budget_authority_unobligat_fyb is also populated
    """

    sf_1 = SF133Factory(line=1000, tas=_TAS, period=1, fiscal_year=2016, amount=1, disaster_emergency_fund_code='Q')
    sf_2 = SF133Factory(line=1000, tas=_TAS, period=1, fiscal_year=2016, amount=1, disaster_emergency_fund_code='B')
    ap = AppropriationFactory(tas=_TAS, budget_authority_unobligat_fyb=1)

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, ap]) == 0


def test_success_fyb_none(database):
    sf = SF133Factory(line=1000, tas=_TAS, period=1, fiscal_year=2016, amount=0)
    ap = AppropriationFactory(tas=_TAS, budget_authority_unobligat_fyb=None)

    assert number_of_errors(_FILE, database, models=[sf, ap]) == 0


def test_success_fyb_sum_zero(database):
    """ Testing that the rule passes if the sum of the SF133 lines is 0 """
    sf_1 = SF133Factory(line=1000, tas=_TAS, period=1, fiscal_year=2016, amount=1, disaster_emergency_fund_code='Q')
    sf_2 = SF133Factory(line=1000, tas=_TAS, period=1, fiscal_year=2016, amount=-1, disaster_emergency_fund_code='B')
    ap = AppropriationFactory(tas=_TAS, budget_authority_unobligat_fyb=None)

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, ap]) == 0


def test_failure(database):
    """ Tests that if the SF-133 amount for line 1000 for the same fiscal year and period is populated, then
        Appropriation budget_authority_unobligat_fyb is not populated. Only one error if there are 2 entries for the
        same row
    """

    sf_1 = SF133Factory(line=1000, tas=_TAS, period=1, fiscal_year=2016, amount=1, disaster_emergency_fund_code='Q')
    sf_2 = SF133Factory(line=1000, tas=_TAS, period=1, fiscal_year=2016, amount=1, disaster_emergency_fund_code='B')
    ap = AppropriationFactory(tas=_TAS, budget_authority_unobligat_fyb=None)

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, ap]) == 1
