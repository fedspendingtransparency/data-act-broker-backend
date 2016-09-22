from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a28_appropriations'
_TAS = 'a28_appropriations_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'other_budgetary_resources_cpe', 'borrowing_authority_amount_cpe',
                       'contract_authority_amount_cpe', 'spending_authority_from_of_cpe'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that OtherBudgetaryResourcesAmount_CPE is provided if TAS has borrowing,
    contract, and/or spending authority provided in File A. """
    tas = "".join([_TAS, "_success"])

    ap1 = AppropriationFactory(tas=tas, other_budgetary_resources_cpe=1, borrowing_authority_amount_cpe=1,
                               contract_authority_amount_cpe=0, spending_authority_from_of_cpe=0)
    ap2 = AppropriationFactory(tas=tas, other_budgetary_resources_cpe=1, borrowing_authority_amount_cpe=0,
                               contract_authority_amount_cpe=1, spending_authority_from_of_cpe=0)
    ap3 = AppropriationFactory(tas=tas, other_budgetary_resources_cpe=1, borrowing_authority_amount_cpe=0,
                               contract_authority_amount_cpe=0, spending_authority_from_of_cpe=1)
    ap4 = AppropriationFactory(tas=tas, other_budgetary_resources_cpe=0, borrowing_authority_amount_cpe=0,
                               contract_authority_amount_cpe=0, spending_authority_from_of_cpe=0)

    assert number_of_errors(_FILE, database, models=[ap1, ap2, ap3, ap4]) == 0


def test_failure(database):
    """ Tests that OtherBudgetaryResourcesAmount_CPE is not provided if TAS has borrowing,
    contract, and/or spending authority provided in File A. """
    tas = "".join([_TAS, "_failure"])

    ap1 = AppropriationFactory(tas=tas, other_budgetary_resources_cpe=0, borrowing_authority_amount_cpe=1,
                               contract_authority_amount_cpe=0, spending_authority_from_of_cpe=0)
    ap2 = AppropriationFactory(tas=tas, other_budgetary_resources_cpe=0, borrowing_authority_amount_cpe=0,
                               contract_authority_amount_cpe=1, spending_authority_from_of_cpe=0)
    ap3 = AppropriationFactory(tas=tas, other_budgetary_resources_cpe=0, borrowing_authority_amount_cpe=0,
                               contract_authority_amount_cpe=0, spending_authority_from_of_cpe=1)

    assert number_of_errors(_FILE, database, models=[ap1, ap2, ap3]) == 3
