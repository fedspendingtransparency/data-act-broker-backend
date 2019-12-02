from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a32_appropriations'


def test_column_headers(database):
    expected_subset = {'uniqueid_TAS', 'row_number', 'allocation_transfer_agency', 'agency_identifier',
                       'beginning_period_of_availa', 'ending_period_of_availabil',
                       'availability_type_code', 'main_account_code', 'sub_account_code'}
    actual = set(query_columns(_FILE, database))
    assert actual == expected_subset


def test_success(database):
    """ Tests that TAS values in File A are not unique """
    tas1 = TASFactory()
    tas2 = TASFactory()
    database.session.add_all([tas1, tas2])
    database.session.flush()

    ap1 = AppropriationFactory(row_number=1, tas_id=tas1.tas_id)
    ap2 = AppropriationFactory(row_number=2, tas_id=tas2.tas_id)

    assert number_of_errors(_FILE, database, models=[ap1, ap2]) == 0


def test_failure(database):
    """ Tests that TAS values in File A are unique """
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()

    ap1 = AppropriationFactory(row_number=1, tas_id=tas.tas_id)
    ap2 = AppropriationFactory(row_number=2, tas_id=tas.tas_id)
    assert number_of_errors(_FILE, database, models=[ap1, ap2]) == 2
