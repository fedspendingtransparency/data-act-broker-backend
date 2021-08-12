from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.domain import DEFCFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'c25_award_financial'


def test_column_headers(database):
    expected_subset = {'row_number', 'disaster_emergency_fund_code', 'transaction_obligated_amou',
                       'gross_outlay_amount_by_awa_cpe', 'uniqueid_TAS', 'uniqueid_DisasterEmergencyFundCode'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test Prior to FY22, if the DisasterEmergencyFundCode element has a valid COVID-19 related code and the row is a
        balance row, then GrossOutlayAmountByAward_CPE cannot be blank. Beginning in FY22, if the row is a balance row,
        then GrossOutlayAmountByAward_CPE cannot be blank.
    """
    # gross_outlay_amount_by_awa_cpe populated
    op1 = AwardFinancialFactory(disaster_emergency_fund_code='l', transaction_obligated_amou=None,
                                gross_outlay_amount_by_awa_cpe=2)
    # 0 in either field is still populated
    op2 = AwardFinancialFactory(disaster_emergency_fund_code='m', transaction_obligated_amou=0,
                                gross_outlay_amount_by_awa_cpe=None)
    op3 = AwardFinancialFactory(disaster_emergency_fund_code='m', transaction_obligated_amou=None,
                                gross_outlay_amount_by_awa_cpe=0)
    # wrong DEFC
    op4 = AwardFinancialFactory(disaster_emergency_fund_code='z', transaction_obligated_amou=None,
                                gross_outlay_amount_by_awa_cpe=None)
    # DEFC but not COVID
    op5 = AwardFinancialFactory(disaster_emergency_fund_code='a', transaction_obligated_amou=None,
                                gross_outlay_amount_by_awa_cpe=None)
    # populated TOA
    op6 = AwardFinancialFactory(disaster_emergency_fund_code='n', transaction_obligated_amou=1,
                                gross_outlay_amount_by_awa_cpe=None)
    defc1 = DEFCFactory(code='L', group='covid_19')
    defc2 = DEFCFactory(code='M', group='covid_19')
    defc3 = DEFCFactory(code='N', group='covid_19')
    defc4 = DEFCFactory(code='A')

    errors = number_of_errors(_FILE, database, models=[op1, op2, op3, op4, op5, op6, defc1, defc2, defc3, defc4])
    assert errors == 0

    # Testing for a submission after 2022
    sub = SubmissionFactory(submission_id=2, reporting_fiscal_period=9, reporting_fiscal_year=2022,
                                   cgac_code='TEST', frec_code=None)
    op1 = AwardFinancialFactory(disaster_emergency_fund_code='p', transaction_obligated_amou=None,
                                gross_outlay_amount_by_awa_cpe=2, submission_id=2)

    errors = number_of_errors(_FILE, database, models=[op1], submission=sub)
    assert errors == 0


def test_failure(database):
    """ Test fail Prior to FY22, if the DisasterEmergencyFundCode element has a valid COVID-19 related code and the row
        is a balance row, then GrossOutlayAmountByAward_CPE cannot be blank. Beginning in FY22, if the row is a balance
        row, then GrossOutlayAmountByAward_CPE cannot be blank.
    """
    op1 = AwardFinancialFactory(disaster_emergency_fund_code='p', transaction_obligated_amou=None,
                                gross_outlay_amount_by_awa_cpe=None)
    defc1 = DEFCFactory(code='P', group='covid_19')

    errors = number_of_errors(_FILE, database, models=[op1, defc1])
    assert errors == 1

    # Testing for a submission after 2022
    sub = SubmissionFactory(submission_id=4, reporting_fiscal_period=9, reporting_fiscal_year=2022,
                            cgac_code='TEST', frec_code=None)
    op1 = AwardFinancialFactory(disaster_emergency_fund_code='t', transaction_obligated_amou=None,
                                gross_outlay_amount_by_awa_cpe=None, submission_id=4)

    errors = number_of_errors(_FILE, database, models=[op1], submission=sub)
    assert errors == 1
