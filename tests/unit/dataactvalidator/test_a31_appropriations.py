from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a31_appropriations'
_TAS = 'a31_appropriations_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'availability_type_code',
                       'beginning_period_of_availa', 'ending_period_of_availabil'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that Beginning Period of Availability and Ending Period of Availability are blank
    if Availability Type Code = X """
    tas = "".join([_TAS, "_success"])

    ap1 = AppropriationFactory(availability_type_code='x', beginning_period_of_availa=None,
                               ending_period_of_availabil=None)
    ap2 = AppropriationFactory(availability_type_code='X', beginning_period_of_availa=None,
                               ending_period_of_availabil=None)

    assert number_of_errors(_FILE, database, models=[ap1, ap2]) == 0


def test_failure(database):
    """ Tests that Beginning Period of Availability and Ending Period of Availability are not blank
    if Availability Type Code = X """
    tas = "".join([_TAS, "_failure"])

    ap1 = AppropriationFactory(availability_type_code='x', beginning_period_of_availa='Today',
                               ending_period_of_availabil='Today')
    ap2 = AppropriationFactory(availability_type_code='x', beginning_period_of_availa='Today',
                               ending_period_of_availabil=None)
    ap3 = AppropriationFactory(availability_type_code='x', beginning_period_of_availa=None,
                               ending_period_of_availabil='Today')
    ap4 = AppropriationFactory(availability_type_code='X', beginning_period_of_availa='Today',
                               ending_period_of_availabil='Today')
    ap5 = AppropriationFactory(availability_type_code='X', beginning_period_of_availa='Today',
                               ending_period_of_availabil=None)
    ap6 = AppropriationFactory(availability_type_code='X', beginning_period_of_availa=None,
                               ending_period_of_availabil='Today')

    assert number_of_errors(_FILE, database, models=[ap1, ap2, ap3, ap4, ap5, ap6]) == 6
