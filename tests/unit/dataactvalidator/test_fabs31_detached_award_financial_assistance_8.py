from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import DUNS
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs31_detached_award_financial_assistance_8'


def test_column_headers(database):
    expected_subset = {'row_number', 'awardee_or_recipient_uniqu', 'uei', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_pubished_date_success(database):
    """
        Test success for when both the AwardeeOrRecipientDUNS and AwardeeOrRecipientUEI are provided, they must match
        the combination shown in SAM for the same awardee or recipient. In this instance, they do not.
    """
    duns_1 = DUNS(awardee_or_recipient_uniqu='111111111', uei='111111111111')
    duns_2 = DUNS(awardee_or_recipient_uniqu='222222222', uei='222222222222')

    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='111111111', uei='111111111111',
                                                          correction_delete_indicatr='')
    # Ignore if delete
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='111111111', uei='222222222222',
                                                          correction_delete_indicatr='d')
    # Ignore if one is just provided
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='111111111', uei=None,
                                                          correction_delete_indicatr='')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='', uei='222222222222',
                                                          correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[duns_1, duns_2, det_award_1, det_award_2, det_award_3,
                                                       det_award_4])
    assert errors == 0


def test_pubished_date_failure(database):
    """
        Test failure for when both the AwardeeOrRecipientDUNS and AwardeeOrRecipientUEI are provided, they must match
        the combination shown in SAM for the same awardee or recipient. In this instance, they do not.
    """
    duns_1 = DUNS(awardee_or_recipient_uniqu='111111111', uei='111111111111')
    duns_2 = DUNS(awardee_or_recipient_uniqu='222222222', uei='222222222222')
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='111111111', uei='2222222222222',
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='111111111', uei='333333333333',
                                                          correction_delete_indicatr='')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='333333333', uei='111111111111',
                                                          correction_delete_indicatr='')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='333333333', uei='333333333333',
                                                          correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[duns_1, duns_2, det_award_1, det_award_2, det_award_3,
                                                       det_award_4])
    assert errors == 4
