from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.staging import AwardFinancialFactory, ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b20_object_class_program_activity_2_1'


def test_column_headers(database):
    expected_subset = {'source_row_number', 'source_value_tas', 'source_value_program_activity_reporting_key',
                       'source_value_object_class', 'source_value_disaster_emergency_fund_code',
                       'source_value_prior_year_adjustment', 'uniqueid_TAS',
                       'uniqueid_ProgramActivityReportingKey', 'uniqueid_ObjectClass',
                       'uniqueid_DisasterEmergencyFundCode'}
    actual = set(query_columns(_FILE, database))
    assert actual == expected_subset


def test_success(database):
    """
        Test all combinations of TAS, PARK, object class, DEFC, and PYA in File C (award financial) must exist in
        File B (object class program activity). Since not all object classes will have award activity, it is acceptable
        for combinations of TAS, PARK, object class, and DEFC combination where PYA = X or NULL in File C to be a subset
        of those provided in File B. (PYA X version)
    """
    tas = TASFactory()
    tas2 = TASFactory()
    database.session.add_all([tas, tas2])
    database.session.flush()

    op = ObjectClassProgramActivityFactory(account_num=tas.account_num, program_activity_reporting_key='1',
                                           object_class='1', disaster_emergency_fund_code='S',
                                           prior_year_adjustment='x')
    op2 = ObjectClassProgramActivityFactory(account_num=tas2.account_num, program_activity_reporting_key='2',
                                            object_class='0', disaster_emergency_fund_code='s',
                                            prior_year_adjustment='X')

    af = AwardFinancialFactory(account_num=tas.account_num, program_activity_reporting_key='1', object_class='1',
                               disaster_emergency_fund_code='s', prior_year_adjustment='x')
    # Allow different object classes if PARKs are the same and tas IDs are the same and object classes are just
    # different numbers of zeroes
    af2 = AwardFinancialFactory(account_num=tas2.account_num, program_activity_reporting_key='2', object_class='00',
                                disaster_emergency_fund_code='s', prior_year_adjustment='x')
    # Ignored because PYA is not X
    af3 = AwardFinancialFactory(account_num=tas.account_num, program_activity_reporting_key='1', object_class='9',
                                disaster_emergency_fund_code='s', prior_year_adjustment='b')
    af4 = AwardFinancialFactory(account_num=tas.account_num, program_activity_reporting_key='1', object_class='9',
                                disaster_emergency_fund_code='s', prior_year_adjustment=None)
    # Ignored because PARK is NULL
    af5 = AwardFinancialFactory(account_num=tas.account_num, program_activity_reporting_key='', object_class='9',
                                disaster_emergency_fund_code='s', prior_year_adjustment='x')
    af6 = AwardFinancialFactory(account_num=tas.account_num, program_activity_reporting_key=None, object_class='9',
                                disaster_emergency_fund_code='s', prior_year_adjustment='x')

    assert number_of_errors(_FILE, database, models=[op, op2, af, af2, af3, af4, af5, af6]) == 0


def test_failure(database):
    """
        Test fail all combinations of TAS, PARK, object class, DEFC, and PYA in File C (award financial) must exist in
        File B (object class program activity). Since not all object classes will have award activity, it is acceptable
        for combinations of TAS, PARK, object class, and DEFC combination where PYA = X or NULL in File C to be a subset
        of those provided in File B. (PYA X version)
    """
    tas1 = TASFactory()
    tas2 = TASFactory()
    tas3 = TASFactory()
    database.session.add_all([tas1, tas2, tas3])
    database.session.flush()

    op = ObjectClassProgramActivityFactory(account_num=tas1.account_num, program_activity_reporting_key='1',
                                           object_class='1', disaster_emergency_fund_code='s',
                                           prior_year_adjustment='x')
    op2 = ObjectClassProgramActivityFactory(account_num=tas2.account_num, program_activity_reporting_key='1',
                                            object_class='2', disaster_emergency_fund_code='s',
                                            prior_year_adjustment='X')
    # Ignored because PYA is not X
    op3 = ObjectClassProgramActivityFactory(account_num=tas2.account_num, program_activity_reporting_key='5',
                                            object_class='0', disaster_emergency_fund_code='s',
                                            prior_year_adjustment='b')

    # Different account num
    af1 = AwardFinancialFactory(account_num=tas3.account_num, program_activity_code='1', program_activity_name='PA1',
                                object_class='1', disaster_emergency_fund_code='s', prior_year_adjustment='x')
    # Different PARK
    af2 = AwardFinancialFactory(account_num=tas1.account_num, program_activity_reporting_key='5', object_class='1',
                                disaster_emergency_fund_code='s', prior_year_adjustment='x')
    # Different Object Class
    af3 = AwardFinancialFactory(account_num=tas1.account_num, program_activity_reporting_key='1', object_class='2',
                                disaster_emergency_fund_code='s', prior_year_adjustment='x')
    # Different DEFC
    af4 = AwardFinancialFactory(account_num=tas1.account_num, program_activity_reporting_key='1', object_class='1',
                                disaster_emergency_fund_code='t', prior_year_adjustment='x')
    # Should error even if object class is 0 because it doesn't match the object class of the op
    af5 = AwardFinancialFactory(account_num=tas2.account_num, program_activity_reporting_key='1', object_class='0',
                                disaster_emergency_fund_code='s', prior_year_adjustment='x')
    # Same values as the PYA that is being ignored but with X
    af6 = AwardFinancialFactory(account_num=tas2.account_num, program_activity_reporting_key='5', object_class='0',
                                disaster_emergency_fund_code='s', prior_year_adjustment='x')

    assert number_of_errors(_FILE, database, models=[op, op2, op3, af1, af2, af3, af4, af5, af6]) == 6
