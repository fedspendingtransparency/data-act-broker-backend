from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'c25_award_financial'


def test_column_headers(database):
    expected_subset = {'row_number', 'disaster_emergency_fund_code', 'transaction_obligated_amou',
                       'gross_outlay_amount_by_awa_cpe', 'uniqueid_TAS', 'uniqueid_DisasterEmergencyFundCode'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test if the DisasterEmergencyFundCode element has a valid COVID-19 related code and TOA is blank, then
        GrossOutlayByAward_CPE cannot be blank.
    """
    # gross_outlay_amount_by_awa_cpe populated
    op1 = AwardFinancialFactory(disaster_emergency_fund_code='a', transaction_obligated_amou=0,
                                gross_outlay_amount_by_awa_cpe=2)
    # 0 is still populated
    op2 = AwardFinancialFactory(disaster_emergency_fund_code='a', transaction_obligated_amou=0,
                                gross_outlay_amount_by_awa_cpe=0)
    # wrong DEFC
    op3 = AwardFinancialFactory(disaster_emergency_fund_code='z', transaction_obligated_amou=None,
                                gross_outlay_amount_by_awa_cpe=0)
    # populated TOA
    op4 = AwardFinancialFactory(disaster_emergency_fund_code='9', transaction_obligated_amou=1,
                                gross_outlay_amount_by_awa_cpe=None)

    errors = number_of_errors(_FILE, database, models=[op1, op2, op3, op4])
    assert errors == 0


def test_failure(database):
    """ Test fail if the DisasterEmergencyFundCode element has a valid COVID-19 related code and TOA is blank, then
        GrossOutlayByAward_CPE cannot be blank.
    """
    op1 = AwardFinancialFactory(disaster_emergency_fund_code='a', transaction_obligated_amou=None,
                                gross_outlay_amount_by_awa_cpe=None)

    errors = number_of_errors(_FILE, database, models=[op1])
    assert errors == 1
