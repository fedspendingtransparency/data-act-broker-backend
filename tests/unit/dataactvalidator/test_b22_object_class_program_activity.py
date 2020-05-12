from tests.unit.dataactcore.factories.domain import SF133Factory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b22_object_class_program_activity'


def test_column_headers(database):
    expected_subset = {'uniqueid_TAS', 'uniqueid_DisasterEmergencyFundCode', 'row_number',
                       'gross_outlay_amount_by_pro_cpe', 'expected_value_GTAS SF133 Line 3020', 'difference'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ GrossOutlayAmountByProgramObjectClass_CPE = value for GTAS SF 133 line #3020 for the same reporting period for
        the TAS and DEFC combination (except for when '9' is provided as DEFC).
    """
    submission_id = 1
    tas, period, year = 'some-tas', 2, 2002

    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year)
    sf = SF133Factory(line=3020, tas=tas, period=period, fiscal_year=year, amount=1, disaster_emergency_fund_code='N')
    op = ObjectClassProgramActivityFactory(submission_id=submission_id, row_number=1, tas=tas,
                                           gross_outlay_amount_by_pro_cpe=1, disaster_emergency_fund_code='n')

    assert number_of_errors(_FILE, database, models=[sf, op], submission=submission) == 0


def test_non_matching_defc(database):
    """ GrossOutlayAmountByProgramObjectClass_CPE = value for GTAS SF 133 line #3020 for the same reporting period for
        the TAS and DEFC combination (except for when '9' is provided as DEFC). Entries with different DEFC ignored
    """
    submission_id = 1
    tas, period, year = 'some-tas', 2, 2002

    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year)

    sf_1 = SF133Factory(line=3020, tas=tas, period=period, fiscal_year=year, amount=1, disaster_emergency_fund_code='N')
    sf_2 = SF133Factory(line=3020, tas=tas, period=period, fiscal_year=year, amount=2, disaster_emergency_fund_code='M')
    op = ObjectClassProgramActivityFactory(submission_id=submission_id, row_number=1, tas=tas,
                                           gross_outlay_amount_by_pro_cpe=1, disaster_emergency_fund_code='n')

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, op], submission=submission) == 0


def test_defc_nine(database):
    """ GrossOutlayAmountByProgramObjectClass_CPE = value for GTAS SF 133 line #3020 for the same reporting period for
        the TAS and DEFC combination (except for when '9' is provided as DEFC). Entries with DEFC = 9 ignored
    """
    submission_id = 1
    tas, period, year = 'some-tas', 2, 2002

    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year)
    # Disaster emergency fund code of 9 is ignored
    sf = SF133Factory(line=3020, tas=tas, period=period, fiscal_year=year, amount=1, disaster_emergency_fund_code='N')
    op = ObjectClassProgramActivityFactory(submission_id=submission_id, row_number=1, tas=tas,
                                           gross_outlay_amount_by_pro_cpe=0, disaster_emergency_fund_code='9')

    assert number_of_errors(_FILE, database, models=[sf, op], submission=submission) == 0


def test_failure(database):
    """ Fail GrossOutlayAmountByProgramObjectClass_CPE = value for GTAS SF 133 line #3020 for the same reporting period
        for the TAS and DEFC combination (except for when '9' is provided as DEFC).
    """
    submission_id = 1
    tas, period, year = 'some-tas', 2, 2002

    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year)
    sf = SF133Factory(line=3020, tas=tas, period=period, fiscal_year=year, amount=1, disaster_emergency_fund_code='N')
    op = ObjectClassProgramActivityFactory(submission_id=submission_id, row_number=1, tas=tas,
                                           gross_outlay_amount_by_pro_cpe=0, disaster_emergency_fund_code='n')

    assert number_of_errors(_FILE, database, models=[sf, op], submission=submission) == 1
