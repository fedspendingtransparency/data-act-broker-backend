from random import randint
from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'a1_object_class_program_activity'

def test_column_headers(database):
    expected_subset = {"row_number", "allocation_transfer_agency", "agency_identifier", "beginning_period_of_availa",
        "ending_period_of_availabil", "availability_type_code", "main_account_code", "sub_account_code"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual

def test_success(database):
    """ Test that TAS values can be found, and null matches work correctly"""
    tas = TASFactory()
    tas_null = TASFactory(allocation_transfer_agency = None, availability_type_code = None, sub_account_code = None)
    ocpa = ObjectClassProgramActivityFactory(allocation_transfer_agency = tas.allocation_transfer_agency,
                                  agency_identifier = tas.agency_identifier,
                                  beginning_period_of_availa = tas.beginning_period_of_availability,
                                  ending_period_of_availabil = tas.ending_period_of_availability,
                                  availability_type_code = tas.availability_type_code,
                                  main_account_code = tas.main_account_code,
                                  sub_account_code = tas.sub_account_code)
    ocpa_null = ObjectClassProgramActivityFactory(allocation_transfer_agency = tas_null.allocation_transfer_agency,
                                       agency_identifier = tas_null.agency_identifier,
                                       beginning_period_of_availa = tas_null.beginning_period_of_availability,
                                       ending_period_of_availabil = tas_null.ending_period_of_availability,
                                       availability_type_code = tas_null.availability_type_code,
                                       main_account_code = tas_null.main_account_code,
                                       sub_account_code = tas_null.sub_account_code)

    errors = number_of_errors(_FILE, database, models=[tas, tas_null, ocpa, ocpa_null])
    assert errors == 0

def test_failure(database):
    """ Test that tas that does not match is an error"""

    tas = TASFactory(agency_identifier = randint(1,100))
    tas_null = TASFactory(agency_identifier = randint(1,100), allocation_transfer_agency = None, availability_type_code = None, sub_account_code = None)
    ocpa = ObjectClassProgramActivityFactory(allocation_transfer_agency = tas.allocation_transfer_agency,
                                  agency_identifier = randint(101,200),
                                  beginning_period_of_availa = tas.beginning_period_of_availability,
                                  ending_period_of_availabil = tas.ending_period_of_availability,
                                  availability_type_code = tas.availability_type_code,
                                  main_account_code = tas.main_account_code,
                                  sub_account_code = tas.sub_account_code)
    ocpa_null = ObjectClassProgramActivityFactory(allocation_transfer_agency = tas_null.allocation_transfer_agency,
                                       agency_identifier = randint(101,200),
                                       beginning_period_of_availa = tas_null.beginning_period_of_availability,
                                       ending_period_of_availabil = tas_null.ending_period_of_availability,
                                       availability_type_code = tas_null.availability_type_code,
                                       main_account_code = tas_null.main_account_code,
                                       sub_account_code = tas_null.sub_account_code)

    # Non-overlapping ranges of agency IDs should generate two errors
    errors = number_of_errors(_FILE, database, models=[tas, tas_null, ocpa, ocpa_null])
    assert errors == 2