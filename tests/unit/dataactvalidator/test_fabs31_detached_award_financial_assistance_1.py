from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs31_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'record_type', 'business_types', 'awardee_or_recipient_uniqu',
                       'business_types', 'record_type', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ AwardeeOrRecipientUniqueIdentifier Field must be blank for aggregate and PII-redacted non-aggregate records
        (RecordType=1 or 3) and individual recipients (BusinessTypes includes 'P').
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(record_type=1, business_types='ABP',
                                                          awardee_or_recipient_uniqu='',
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=1, business_types='ABC',
                                                          awardee_or_recipient_uniqu=None,
                                                          correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(record_type=3, business_types='ABP',
                                                          awardee_or_recipient_uniqu='',
                                                          correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(record_type=3, business_types='ABC',
                                                          awardee_or_recipient_uniqu=None,
                                                          correction_delete_indicatr='C')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='pbc',
                                                          awardee_or_recipient_uniqu=None,
                                                          correction_delete_indicatr='')
    det_award_6 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='PBC',
                                                          awardee_or_recipient_uniqu='',
                                                          correction_delete_indicatr='')
    det_award_7 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='apc',
                                                          awardee_or_recipient_uniqu='',
                                                          correction_delete_indicatr='')
    det_award_8 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='APC',
                                                          awardee_or_recipient_uniqu=None,
                                                          correction_delete_indicatr='')
    det_award_9 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='abp',
                                                          awardee_or_recipient_uniqu='',
                                                          correction_delete_indicatr='')
    det_award_10 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='ABP',
                                                           awardee_or_recipient_uniqu=None,
                                                           correction_delete_indicatr='')
    det_award_11 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='ABC',
                                                           awardee_or_recipient_uniqu='test',
                                                           correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    det_award_12 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='ABP',
                                                           awardee_or_recipient_uniqu='test',
                                                           correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, det_award_7, det_award_8, det_award_9, det_award_10,
                                                       det_award_11, det_award_12])
    assert errors == 0


def test_failure(database):
    """ Test Failure for AwardeeOrRecipientUniqueIdentifier Field must be blank for aggregate and PII-redacted
        non-aggregate records (RecordType=1 or 3) and individual recipients (BusinessTypes includes 'P').
    """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(record_type=1, business_types='ABC',
                                                          awardee_or_recipient_uniqu='test',
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=3, business_types='ABC',
                                                          awardee_or_recipient_uniqu='test',
                                                          correction_delete_indicatr='C')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='pbc',
                                                          awardee_or_recipient_uniqu='test',
                                                          correction_delete_indicatr=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='PBC',
                                                          awardee_or_recipient_uniqu='test',
                                                          correction_delete_indicatr='c')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='apc',
                                                          awardee_or_recipient_uniqu='test',
                                                          correction_delete_indicatr='')
    det_award_6 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='APC',
                                                          awardee_or_recipient_uniqu='test',
                                                          correction_delete_indicatr='')
    det_award_7 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='abp',
                                                          awardee_or_recipient_uniqu='test',
                                                          correction_delete_indicatr='')
    det_award_8 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='ABP',
                                                          awardee_or_recipient_uniqu='test',
                                                          correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, det_award_7, det_award_8])
    assert errors == 8
