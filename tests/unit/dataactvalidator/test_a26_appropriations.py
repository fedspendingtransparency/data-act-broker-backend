from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactcore.factories.domain import SF133Factory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a26_appropriations'
_TAS = 'a26_appropriations_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'contract_authority_amount_cpe',
                       'lines', 'amounts'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that ContractAuthorityAmountTotal_CPE is provided if TAS has contract authority value
    provided in GTAS """
    tas = "".join([_TAS, "_success"])

    sf1 = SF133Factory(tas=tas, period=1, fiscal_year=2016, line=1540, amount=1)
    sf2 = SF133Factory(tas=tas, period=1, fiscal_year=2016, line=1640, amount=1)

    ap = AppropriationFactory(tas=tas, contract_authority_amount_cpe=1)

    assert number_of_errors(_FILE, database, models=[sf1, sf2, ap]) == 0


def test_failure(database):
    """ Tests that ContractAuthorityAmountTotal_CPE is not provided if TAS has contract authority value
    provided in GTAS """
    tas = "".join([_TAS, "_failure"])

    sf1 = SF133Factory(tas=tas, period=1, fiscal_year=2016, line=1540, amount=1)
    sf2 = SF133Factory(tas=tas, period=1, fiscal_year=2016, line=1640, amount=1)

    ap = AppropriationFactory(tas=tas, contract_authority_amount_cpe=0)

    assert number_of_errors(_FILE, database, models=[sf1, sf2, ap]) == 1
