from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import (
    AwardFinancialFactory, ObjectClassProgramActivityFactory)
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b20_object_class_program_activity'


def test_column_headers(database):
    expected_subset = {'source_row_number', 'source_value_program_activity_code', 'source_value_program_activity_name',
                       'source_value_object_class', 'source_value_disaster_emergency_fund_code', 'uniqueid_TAS',
                       'uniqueid_ProgramActivityCode', 'uniqueid_ProgramActivityName', 'uniqueid_ObjectClass',
                       'uniqueid_DisasterEmergencyFundCode'}
    actual = set(query_columns(_FILE, database))
    assert actual == expected_subset


def test_success(database):
    """ Test All combinations of TAS/program activity code+name/object class/DEFC in File C (award financial) should
        exist in File B (object class program activity). Since not all object classes will have award activity, it is
        acceptable for combinations of TAS/program activity code+name/object class/DEFC in File C to be a subset of
        those provided in File B.
    """
    tas = TASFactory()
    tas2 = TASFactory()
    database.session.add_all([tas, tas2])
    database.session.flush()

    op = ObjectClassProgramActivityFactory(account_num=tas.account_num, program_activity_code='1',
                                           program_activity_name='PA1', object_class='1',
                                           disaster_emergency_fund_code='S')
    op2 = ObjectClassProgramActivityFactory(account_num=tas2.account_num, program_activity_code='2',
                                            program_activity_name='PA2', object_class='0',
                                            disaster_emergency_fund_code='s')

    af = AwardFinancialFactory(account_num=tas.account_num, program_activity_code='1', program_activity_name='PA1',
                               object_class='1', disaster_emergency_fund_code='s')
    # Allow program activity code to be null, empty, or zero
    af2 = AwardFinancialFactory(account_num=tas.account_num, program_activity_code='', program_activity_name='Pa1',
                                object_class='1', disaster_emergency_fund_code='S')
    af3 = AwardFinancialFactory(account_num=tas.account_num, program_activity_code='0000', program_activity_name='pA1',
                                object_class='1', disaster_emergency_fund_code='s')
    af4 = AwardFinancialFactory(account_num=tas.account_num, program_activity_code=None, program_activity_name='pa1',
                                object_class='1', disaster_emergency_fund_code='s')
    # Allow program activity name to be null or empty
    af5 = AwardFinancialFactory(account_num=tas.account_num, program_activity_code='', program_activity_name='',
                                object_class='1', disaster_emergency_fund_code='S')
    af6 = AwardFinancialFactory(account_num=tas.account_num, program_activity_code='0000', program_activity_name=None,
                                object_class='1', disaster_emergency_fund_code='s')
    # Allow different object classes if pacs are the same and tas IDs are the same and object classes are just
    # different numbers of zeroes
    af7 = AwardFinancialFactory(account_num=tas2.account_num, program_activity_code='2', program_activity_name='pa2',
                                object_class='00', disaster_emergency_fund_code='s')

    assert number_of_errors(_FILE, database, models=[op, op2, af, af2, af3, af4, af5, af6, af7]) == 0


def test_success_ignore_optional_before_2021(database):
    """ Ignore AwardFinancial entries that are prior to year 2021 and are indicated by the proper PAC and PAN. """
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()

    op = ObjectClassProgramActivityFactory(account_num=tas.account_num, program_activity_code='1',
                                           program_activity_name='PA1', object_class='1',
                                           disaster_emergency_fund_code='s')

    af = AwardFinancialFactory(account_num=tas.account_num, program_activity_code='1', program_activity_name='PA1',
                               object_class='1', disaster_emergency_fund_code='s')
    af2 = AwardFinancialFactory(account_num=tas.account_num, program_activity_code='OPTN',
                                program_activity_name='FIELD IS optional PRIOR TO FY21', object_class='1',
                                disaster_emergency_fund_code='s')

    sub = SubmissionFactory(reporting_fiscal_year=2020)

    assert number_of_errors(_FILE, database, models=[op, af, af2], submission=sub) == 0


def test_failure(database):
    """ All combinations of TAS/program activity code+name/object class/DEFC in File C (award financial) should
        exist in File B (object class program activity). Since not all object classes will have award activity, it is
        acceptable for combinations of TAS/program activity code+name/object class/DEFC in File C to be a subset of
        those provided in File B.
    """
    tas1 = TASFactory()
    tas2 = TASFactory()
    tas3 = TASFactory()
    database.session.add_all([tas1, tas2, tas3])
    database.session.flush()

    op = ObjectClassProgramActivityFactory(account_num=tas1.account_num, program_activity_code='1',
                                           program_activity_name='PA1', object_class='1',
                                           disaster_emergency_fund_code='s')
    op2 = ObjectClassProgramActivityFactory(account_num=tas2.account_num, program_activity_code='1',
                                            program_activity_name='PA2', object_class='2',
                                            disaster_emergency_fund_code='s')

    # Different account num
    af1 = AwardFinancialFactory(account_num=tas3.account_num, program_activity_code='1', program_activity_name='PA1',
                                object_class='1', disaster_emergency_fund_code='s')
    # Different PAC
    af2 = AwardFinancialFactory(account_num=tas1.account_num, program_activity_code='2', program_activity_name='PA1',
                                object_class='1', disaster_emergency_fund_code='s')
    # Different PA Name
    af3 = AwardFinancialFactory(account_num=tas1.account_num, program_activity_code='1', program_activity_name='PA2',
                                object_class='1', disaster_emergency_fund_code='S')
    # Different Object Class
    af4 = AwardFinancialFactory(account_num=tas1.account_num, program_activity_code='1', program_activity_name='PA1',
                                object_class='2', disaster_emergency_fund_code='s')
    # Different DEFC
    af5 = AwardFinancialFactory(account_num=tas1.account_num, program_activity_code='1', program_activity_name='PA1',
                                object_class='1', disaster_emergency_fund_code='t')
    # Should error even if object class is 0 because it doesn't match the object class of the op
    af6 = AwardFinancialFactory(account_num=tas2.account_num, program_activity_code='1', object_class='0')

    assert number_of_errors(_FILE, database, models=[op, op2, af1, af2, af3, af4, af5, af6]) == 6


def test_fail_ignore_optional_2021(database):
    """ Don't ignore AwardFinancial entries that are year 2021 or later and are indicated by the proper PAC and PAN. """
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()

    op = ObjectClassProgramActivityFactory(account_num=tas.account_num, program_activity_code='1',
                                           program_activity_name='PA1', object_class='1')

    af = AwardFinancialFactory(account_num=tas.account_num, program_activity_code='1', program_activity_name='PA1',
                               object_class='1')
    af2 = AwardFinancialFactory(account_num=tas.account_num, program_activity_code='OPTN',
                                program_activity_name='FIELD IS optional PRIOR TO FY21', object_class='1')

    sub = SubmissionFactory(reporting_fiscal_year=2021)

    assert number_of_errors(_FILE, database, models=[op, af, af2], submission=sub) == 1
