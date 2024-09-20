from tests.unit.dataactcore.factories.domain import SF133Factory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b26_object_class_program_activity'


def test_column_headers(database):
    expected_subset = {'uniqueid_TAS', 'uniqueid_DisasterEmergencyFundCode', 'row_number',
                       'deobligations_recov_by_pro_cpe_sum', 'expected_value_SUM of GTAS SF133 Lines 1021, 1033',
                       'difference'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ DeobligationsRecoveriesRefundsOfPriorYearByProgramObjectClass_CPE = value for GTAS SF 133 lines #1021+1033 for
        the same reporting period for the TAS and DEFC combination where PYA = "X".
    """
    submission_id = 1
    tas, period, year = 'some-tas', 2, 2002

    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year)
    sf_1 = SF133Factory(line=1021, tas=tas, period=period, fiscal_year=year, amount=1, disaster_emergency_fund_code='N')
    sf_2 = SF133Factory(line=1033, tas=tas, period=period, fiscal_year=year, amount=2, disaster_emergency_fund_code='N')
    op = ObjectClassProgramActivityFactory(submission_id=submission_id, row_number=1, tas=tas, display_tas=tas,
                                           deobligations_recov_by_pro_cpe=3, disaster_emergency_fund_code='n',
                                           prior_year_adjustment='X')

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, op], submission=submission) == 0


def test_success_multiple_rows(database):
    """ DeobligationsRecoveriesRefundsOfPriorYearByProgramObjectClass_CPE = value for GTAS SF 133 lines #1021+1033 for
        the same reporting period for the TAS and DEFC combination where PYA = "X".
    """
    submission_id = 1
    tas, period, year = 'some-tas', 2, 2002

    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year)
    sf_1 = SF133Factory(line=1021, tas=tas, period=period, fiscal_year=year, amount=3, disaster_emergency_fund_code='N')
    sf_2 = SF133Factory(line=1033, tas=tas, period=period, fiscal_year=year, amount=2, disaster_emergency_fund_code='N')
    op_1 = ObjectClassProgramActivityFactory(submission_id=submission_id, row_number=1, tas=tas, display_tas=tas,
                                             deobligations_recov_by_pro_cpe=1, disaster_emergency_fund_code='n',
                                           prior_year_adjustment='X')
    op_2 = ObjectClassProgramActivityFactory(submission_id=submission_id, row_number=2, tas=tas, display_tas=tas,
                                             deobligations_recov_by_pro_cpe=4, disaster_emergency_fund_code='n',
                                             prior_year_adjustment='x')

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, op_1, op_2], submission=submission) == 0


def test_non_matching_defc(database):
    """ DeobligationsRecoveriesRefundsOfPriorYearByProgramObjectClass_CPE = value for GTAS SF 133 lines #1021+1033 for
        the same reporting period for the TAS and DEFC combination where PYA = "X". Ignore different DEFC
    """
    submission_id = 1
    tas, period, year = 'some-tas', 2, 2002

    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year)

    sf_1 = SF133Factory(line=1021, tas=tas, period=period, fiscal_year=year, amount=1, disaster_emergency_fund_code='N')
    sf_2 = SF133Factory(line=1033, tas=tas, period=period, fiscal_year=year, amount=0, disaster_emergency_fund_code='N')
    sf_3 = SF133Factory(line=1021, tas=tas, period=period, fiscal_year=year, amount=2, disaster_emergency_fund_code='M')
    sf_4 = SF133Factory(line=1033, tas=tas, period=period, fiscal_year=year, amount=1, disaster_emergency_fund_code='M')
    op = ObjectClassProgramActivityFactory(submission_id=submission_id, row_number=1, tas=tas, display_tas=tas,
                                           deobligations_recov_by_pro_cpe=1, disaster_emergency_fund_code='n',
                                           prior_year_adjustment='X')

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, sf_3, sf_4, op], submission=submission) == 0

def test_different_pya(database):
    """ DeobligationsRecoveriesRefundsOfPriorYearByProgramObjectClass_CPE = value for GTAS SF 133 lines #1021+1033 for
        the same reporting period for the TAS and DEFC combination where PYA = "X". Ignore different PYA
    """
    submission_id = 1
    tas, period, year = 'some-tas', 2, 2002

    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year)
    sf_1 = SF133Factory(line=1021, tas=tas, period=period, fiscal_year=year, amount=1, disaster_emergency_fund_code='N')
    sf_2 = SF133Factory(line=1033, tas=tas, period=period, fiscal_year=year, amount=0, disaster_emergency_fund_code='N')
    op = ObjectClassProgramActivityFactory(submission_id=submission_id, row_number=1, tas=tas, display_tas=tas,
                                           deobligations_recov_by_pro_cpe=0, disaster_emergency_fund_code='n',
                                           prior_year_adjustment='x')

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, op], submission=submission) == 1

def test_failure(database):
    """ Fail DeobligationsRecoveriesRefundsOfPriorYearByProgramObjectClass_CPE = value for GTAS SF 133 lines #1021+1033
        for the same reporting period for the TAS and DEFC combination where PYA = "X".
    """
    submission_id = 1
    tas, period, year = 'some-tas', 2, 2002

    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year)
    sf_1 = SF133Factory(line=1021, tas=tas, period=period, fiscal_year=year, amount=1, disaster_emergency_fund_code='N')
    sf_2 = SF133Factory(line=1033, tas=tas, period=period, fiscal_year=year, amount=0, disaster_emergency_fund_code='N')
    op = ObjectClassProgramActivityFactory(submission_id=submission_id, row_number=1, tas=tas, display_tas=tas,
                                           deobligations_recov_by_pro_cpe=0, disaster_emergency_fund_code='n',
                                           prior_year_adjustment='x')

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, op], submission=submission) == 1
