from tests.unit.dataactcore.factories.domain import SF133Factory
from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a36_appropriations'
_TAS = 'a36_appropriations_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'budget_authority_unobligat_fyb', 'sf_133_amount'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that if the SF-133 amount for line 1000 for the same fiscal year and period is populated, then
    Appropriation budget_authority_unobligat_fyb is also populated"""

    sf = SF133Factory(line=1000, tas=_TAS, period=1, fiscal_year=2016, amount=1)
    ap = AppropriationFactory(tas=_TAS, budget_authority_unobligat_fyb=1)

    assert number_of_errors(_FILE, database, models=[sf, ap]) == 0

    sf = SF133Factory(line=1000, tas=_TAS, period=1, fiscal_year=2016, amount=0)
    ap = AppropriationFactory(tas=_TAS, budget_authority_unobligat_fyb=None)

    assert number_of_errors(_FILE, database, models=[sf, ap]) == 0


def test_failure(database):
    """ Tests that if the SF-133 amount for line 1000 for the same fiscal year and period is populated, then
    Appropriation budget_authority_unobligat_fyb is not populated"""

    sf = SF133Factory(line=1000, tas=_TAS, period=1, fiscal_year=2016, amount=1)
    ap = AppropriationFactory(tas=_TAS, budget_authority_unobligat_fyb=None)

    assert number_of_errors(_FILE, database, models=[sf, ap]) == 1