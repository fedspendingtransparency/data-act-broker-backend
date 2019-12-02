from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.staging import AppropriationFactory, ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a30_object_class_program_activity'


def test_column_headers(database):
    expected_subset = {'source_row_number', 'source_value_allocation_transfer_agency', 'source_value_agency_identifier',
                       'source_value_beginning_period_of_availa', 'source_value_ending_period_of_availabil',
                       'source_value_availability_type_code', 'source_value_main_account_code',
                       'source_value_sub_account_code', 'uniqueid_TAS'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that TAS values in File B should exist in File A for the same reporting period """
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()

    op = ObjectClassProgramActivityFactory(tas_id=tas.tas_id)
    ap = AppropriationFactory(tas_id=tas.tas_id)

    assert number_of_errors(_FILE, database, models=[op, ap]) == 0


def test_failure(database):
    """ Tests that TAS values in File B do not exist in File A for the same reporting period """
    tas1 = TASFactory()
    tas2 = TASFactory()
    database.session.add_all([tas1, tas2])
    database.session.flush()

    op = ObjectClassProgramActivityFactory(tas_id=tas1.tas_id)
    ap = AppropriationFactory(tas_id=tas2.tas_id)

    assert number_of_errors(_FILE, database, models=[op, ap]) == 1
