from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from decimal import Decimal

_FILE = 'a24_appropriations'


def test_column_headers(database):
    expected_subset = {'row_number', 'status_of_budgetary_resour_cpe', 'total_budgetary_resources_cpe', 'difference'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test that calculation works for equal values and for null """
    value = Decimal('100.23')
    approp = AppropriationFactory(status_of_budgetary_resour_cpe=value, total_budgetary_resources_cpe=value)
    approp_null = AppropriationFactory(status_of_budgetary_resour_cpe=0, total_budgetary_resources_cpe=None)

    errors = number_of_errors(_FILE, database, models=[approp, approp_null])
    assert errors == 0


def test_failure(database):
    """ Test that calculation fails for unequal values """
    approp = AppropriationFactory(status_of_budgetary_resour_cpe=Decimal(101.23),
                                  total_budgetary_resources_cpe=Decimal(102.34))

    errors = number_of_errors(_FILE, database, models=[approp])
    assert errors == 1
