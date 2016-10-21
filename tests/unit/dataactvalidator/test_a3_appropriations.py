from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'a3_appropriations'

def test_column_headers(database):
    expected_subset = {"row_number", "other_budgetary_resources_cpe", "contract_authority_amount_cpe",
                       "borrowing_authority_amount_cpe", "spending_authority_from_of_cpe"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual

def test_success(database):
    """ Test that TAS values can be found, and null matches work correctly"""
    approp = AppropriationFactory(other_budgetary_resources_cpe = 600, contract_authority_amount_cpe = 100,
                                  borrowing_authority_amount_cpe = 200, spending_authority_from_of_cpe = 300)
    approp_null = AppropriationFactory(other_budgetary_resources_cpe = 300, contract_authority_amount_cpe = 100,
                                  borrowing_authority_amount_cpe = 200, spending_authority_from_of_cpe = None)

    errors = number_of_errors(_FILE, database, models=[approp, approp_null])
    assert errors == 0

def test_failure(database):
    """ Test that tas that does not match is an error"""

    approp = AppropriationFactory(other_budgetary_resources_cpe = 800, contract_authority_amount_cpe = 100,
                                  borrowing_authority_amount_cpe = 200, spending_authority_from_of_cpe = 300)
    approp_null = AppropriationFactory(other_budgetary_resources_cpe = 500, contract_authority_amount_cpe = 100,
                                  borrowing_authority_amount_cpe = 200, spending_authority_from_of_cpe = None)

    errors = number_of_errors(_FILE, database, models=[approp, approp_null])
    assert errors == 2