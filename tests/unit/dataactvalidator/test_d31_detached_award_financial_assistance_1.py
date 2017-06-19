from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd31_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {"row_number", "record_type", "business_types", "awardee_or_recipient_uniqu"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test success for AwardeeOrRecipientUniqueIdentifier Field must be blank for aggregate records
    (i.e., when RecordType = 1) and individual recipients. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(record_type=1, business_types="ABP",
                                                          awardee_or_recipient_uniqu='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=1, business_types="ABC",
                                                          awardee_or_recipient_uniqu=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types="pbc",
                                                          awardee_or_recipient_uniqu=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types="PBC",
                                                          awardee_or_recipient_uniqu='')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types="apc",
                                                          awardee_or_recipient_uniqu='')
    det_award_6 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types="APC",
                                                          awardee_or_recipient_uniqu=None)
    det_award_7 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types="abp",
                                                          awardee_or_recipient_uniqu='')
    det_award_8 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types="ABP",
                                                          awardee_or_recipient_uniqu=None)
    det_award_9 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types="ABC",
                                                          awardee_or_recipient_uniqu='test')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, det_award_7, det_award_8, det_award_9])
    assert errors == 0


def test_failure(database):
    """ Test failure for AwardeeOrRecipientUniqueIdentifier Field must be blank for aggregate records
    (i.e., when RecordType = 1) and individual recipients. """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(record_type=1, business_types="ABC",
                                                          awardee_or_recipient_uniqu='test')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types="pbc",
                                                          awardee_or_recipient_uniqu='test')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types="PBC",
                                                          awardee_or_recipient_uniqu='test')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types="apc",
                                                          awardee_or_recipient_uniqu='test')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types="APC",
                                                          awardee_or_recipient_uniqu='test')
    det_award_6 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types="abp",
                                                          awardee_or_recipient_uniqu='test')
    det_award_7 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types="ABP",
                                                          awardee_or_recipient_uniqu='test')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, det_award_7])
    assert errors == 7
