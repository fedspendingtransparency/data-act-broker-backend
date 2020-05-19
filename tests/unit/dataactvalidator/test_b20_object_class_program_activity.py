from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.staging import (
    AwardFinancialFactory, ObjectClassProgramActivityFactory)
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b20_object_class_program_activity'


def test_column_headers(database):
    expected_subset = {'source_row_number', 'source_value_program_activity_code', 'source_value_program_activity_name',
                       'source_value_object_class', 'uniqueid_TAS', 'uniqueid_ProgramActivityCode',
                       'uniqueid_ProgramActivityName', 'uniqueid_ObjectClass'}
    actual = set(query_columns(_FILE, database))
    assert actual == expected_subset


def test_success(database):
    """ Test All combinations of TAS/program activity code+name (when provided in File C)/object class in File C
        (award financial) should exist in File B (object class program activity). Since not all object classes will
        have award activity, it is acceptable for combinations of TAS/program activity code+name/object class in File C
        to be a subset of those provided in File B.
    """
    tas = TASFactory()
    tas2 = TASFactory()
    database.session.add_all([tas, tas2])
    database.session.flush()

    op = ObjectClassProgramActivityFactory(tas_id=tas.tas_id, program_activity_code='1', program_activity_name='PA1',
                                           object_class='1')
    op2 = ObjectClassProgramActivityFactory(tas_id=tas2.tas_id, program_activity_code='2', program_activity_name='PA2',
                                            object_class='0')

    af = AwardFinancialFactory(tas_id=tas.tas_id, program_activity_code='1', program_activity_name='PA1',
                               object_class='1')
    # Allow program activity code to be null, empty, or zero
    af2 = AwardFinancialFactory(tas_id=tas.tas_id, program_activity_code='', program_activity_name='Pa1',
                                object_class='1')
    af3 = AwardFinancialFactory(tas_id=tas.tas_id, program_activity_code='0000', program_activity_name='pA1',
                                object_class='1')
    af4 = AwardFinancialFactory(tas_id=tas.tas_id, program_activity_code=None, program_activity_name='pa1',
                                object_class='1')
    # Allow program activity name to be null or empty
    af5 = AwardFinancialFactory(tas_id=tas.tas_id, program_activity_code='', program_activity_name='',
                                object_class='1')
    af6 = AwardFinancialFactory(tas_id=tas.tas_id, program_activity_code='0000', program_activity_name=None,
                                object_class='1')
    # Allow different object classes if pacs are the same and tas IDs are the same and object classes are just
    # different numbers of zeroes
    af7 = AwardFinancialFactory(tas_id=tas2.tas_id, program_activity_code='2', program_activity_name='pa2',
                                object_class='00')

    assert number_of_errors(_FILE, database, models=[op, op2, af, af2, af3, af4, af5, af6, af7]) == 0


def test_failure(database):
    """ All combinations of TAS/program activity code+name (when provided in File C)/object class in File C
        (award financial) should exist in File B (object class program activity). Since not all object classes will have
        award activity, it is acceptable for combinations of TAS/program activity code+name/object class in File C to be
        a subset of those provided in File B.
    """
    tas1 = TASFactory()
    tas2 = TASFactory()
    tas3 = TASFactory()
    database.session.add_all([tas1, tas2, tas3])
    database.session.flush()

    op = ObjectClassProgramActivityFactory(tas_id=tas1.tas_id, program_activity_code='1', program_activity_name='PA1',
                                           object_class='1')
    op2 = ObjectClassProgramActivityFactory(tas_id=tas2.tas_id, program_activity_code='1', program_activity_name='PA2',
                                            object_class='2')

    af1 = AwardFinancialFactory(tas_id=tas3.tas_id, program_activity_code='1', program_activity_name='PA1',
                                object_class='1')
    af2 = AwardFinancialFactory(tas_id=tas1.tas_id, program_activity_code='2', program_activity_name='PA1',
                                object_class='1')
    af3 = AwardFinancialFactory(tas_id=tas1.tas_id, program_activity_code='1', program_activity_name='PA2',
                                object_class='1')
    af4 = AwardFinancialFactory(tas_id=tas1.tas_id, program_activity_code='1', program_activity_name='PA1',
                                object_class='2')
    # Should error even if object class is 0 because it doesn't match the object class of the op
    af5 = AwardFinancialFactory(tas_id=tas2.tas_id, program_activity_code='1', object_class='0')

    assert number_of_errors(_FILE, database, models=[op, op2, af1, af2, af3, af4, af5]) == 5
