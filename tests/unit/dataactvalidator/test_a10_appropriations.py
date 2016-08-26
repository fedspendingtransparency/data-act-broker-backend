from tests.unit.dataactcore.factories.domain import SF133Factory
from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors


_FILE = 'a10_appropriations'
_TAS = 'a10_appropriations_tas'

# @todo: that a10 sql joins to a submission on particular values which aren't
# being set up here?


def test_success(database):
    """ Tests that SF 133 amount sum for lines 1340, 1440 matches Appropriation borrowing_authority_amount_cpe
        for the specified fiscal year and period """
    tas = "".join([_TAS, "_success"])

    sf_1 = SF133Factory(line=1340, tas=tas, period=1, fiscal_year=2016,
                        amount=1)
    sf_2 = SF133Factory(line=1440, tas=tas, period=1, fiscal_year=2016,
                        amount=1)
    ap = AppropriationFactory(tas=tas, borrowing_authority_amount_cpe=2)

    models = [sf_1, sf_2, ap]

    assert number_of_errors(_FILE, database.stagingDb, models=models) == 0


def test_failure(database):
    """ Tests that SF 133 amount sum for lines 1340, 1440 does not match Appropriation borrowing_authority_amount_cpe
        for the specified fiscal year and period """
    tas = "".join([_TAS, "_failure"])

    sf_1 = SF133Factory(line=1340, tas=tas, period=1, fiscal_year=2016,
                        amount=1)
    sf_2 = SF133Factory(line=1440, tas=tas, period=1, fiscal_year=2016,
                        amount=1)
    ap = AppropriationFactory(tas=tas, borrowing_authority_amount_cpe=1)

    models = [sf_1, sf_2, ap]

    assert number_of_errors(_FILE, database.stagingDb, models=models) == 1
