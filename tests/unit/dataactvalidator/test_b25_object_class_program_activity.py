from tests.unit.dataactcore.factories.domain import SF133Factory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b25_object_class_program_activity'


def test_column_headers(database):
    expected_subset = {'uniqueid_TAS', 'uniqueid_DisasterEmergencyFundCode', 'row_number',
                       'obligations_incurred_by_pr_cpe_sum', 'expected_value_GTAS SF133 Line 2190', 'difference'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ ObligationsIncurredByProgramObjectClass_CPE = the negative (additive inverse) value for GTAS SF 133 line #2190
        for the same reporting period for the TAS and DEFC combination.
    """
    submission_id = 1
    tas, period, year = 'some-tas', 2, 2002

    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year)
    sf = SF133Factory(line=2190, tas=tas, period=period, fiscal_year=year, amount=1, disaster_emergency_fund_code='N')
    op = ObjectClassProgramActivityFactory(submission_id=submission_id, row_number=1, tas=tas, display_tas=tas,
                                           obligations_incurred_by_pr_cpe=-1, disaster_emergency_fund_code='n')

    assert number_of_errors(_FILE, database, models=[sf, op], submission=submission) == 0


def test_success_multiple_rows(database):
    """ ObligationsIncurredByProgramObjectClass_CPE = the negative (additive inverse) value for GTAS SF 133 line #2190
        for the same reporting period for the TAS and DEFC combination. Multiple OP rows for the same combo
    """
    submission_id = 1
    tas, period, year = 'some-tas', 2, 2002

    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year)
    sf = SF133Factory(line=2190, tas=tas, period=period, fiscal_year=year, amount=5, disaster_emergency_fund_code='N')
    op_1 = ObjectClassProgramActivityFactory(submission_id=submission_id, row_number=1, tas=tas, display_tas=tas,
                                             obligations_incurred_by_pr_cpe=-1, disaster_emergency_fund_code='n')
    op_2 = ObjectClassProgramActivityFactory(submission_id=submission_id, row_number=2, tas=tas, display_tas=tas,
                                             obligations_incurred_by_pr_cpe=-4, disaster_emergency_fund_code='n')

    assert number_of_errors(_FILE, database, models=[sf, op_1, op_2], submission=submission) == 0


def test_non_matching_defc(database):
    """ ObligationsIncurredByProgramObjectClass_CPE = the negative (additive inverse) value for GTAS SF 133 line #2190
        for the same reporting period for the TAS and DEFC combination. Entries with different DEFC ignored
    """
    submission_id = 1
    tas, period, year = 'some-tas', 2, 2002

    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year)

    sf_1 = SF133Factory(line=2190, tas=tas, period=period, fiscal_year=year, amount=1, disaster_emergency_fund_code='N')
    sf_2 = SF133Factory(line=2190, tas=tas, period=period, fiscal_year=year, amount=2, disaster_emergency_fund_code='M')
    op = ObjectClassProgramActivityFactory(submission_id=submission_id, row_number=1, tas=tas, display_tas=tas,
                                           obligations_incurred_by_pr_cpe=-1, disaster_emergency_fund_code='n')

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, op], submission=submission) == 0


def test_failure(database):
    """ Fail ObligationsIncurredByProgramObjectClass_CPE = the negative (additive inverse) value for GTAS SF 133 line
        #2190 for the same reporting period for the TAS and DEFC combination.
    """
    submission_id = 1
    tas, period, year = 'some-tas', 2, 2002

    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year)
    sf = SF133Factory(line=2190, tas=tas, period=period, fiscal_year=year, amount=1, disaster_emergency_fund_code='N')
    op = ObjectClassProgramActivityFactory(submission_id=submission_id, row_number=1, tas=tas, display_tas=tas,
                                           obligations_incurred_by_pr_cpe=0, disaster_emergency_fund_code='n')

    assert number_of_errors(_FILE, database, models=[sf, op], submission=submission) == 1


def test_failure_same_sign(database):
    """ Fail ObligationsIncurredByProgramObjectClass_CPE = the negative (additive inverse) value for GTAS SF 133 line
        #2190 for the same reporting period for the TAS and DEFC combination. Same sign.
    """
    submission_id = 1
    tas, period, year = 'some-tas', 2, 2002

    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year)
    sf = SF133Factory(line=2190, tas=tas, period=period, fiscal_year=year, amount=1, disaster_emergency_fund_code='N')
    op = ObjectClassProgramActivityFactory(submission_id=submission_id, row_number=1, tas=tas, display_tas=tas,
                                           obligations_incurred_by_pr_cpe=1, disaster_emergency_fund_code='n')

    assert number_of_errors(_FILE, database, models=[sf, op], submission=submission) == 1
