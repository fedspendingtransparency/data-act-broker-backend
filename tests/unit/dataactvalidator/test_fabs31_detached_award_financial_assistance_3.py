from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs31_detached_award_financial_assistance_3'


def test_column_headers(database):
    expected_subset = {'row_number', 'awardee_or_recipient_uniqu', 'uei', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """
        Test success for when AwardeeOrRecipientUEI is provided, it must be twelve characters.
        When AwardeeOrRecipientDUNS is provided, it must be nine digits.
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='000000001', uei='123456789aBc',
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='', uei='123456789aBc',
                                                          correction_delete_indicatr='c')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='103493922', uei='abc000000000',
                                                          correction_delete_indicatr='C')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='100000000', uei='000000000000',
                                                          correction_delete_indicatr=None)
    det_award_5 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='100000000', uei=None,
                                                          correction_delete_indicatr=None)
    det_award_6 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='', uei='000000000000',
                                                          correction_delete_indicatr=None)
    det_award_7 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='', uei=None,
                                                          correction_delete_indicatr=None)

    # Ignore correction delete indicator of D
    det_award_8 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='00000000A', uei='2',
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4,
                                                       det_award_5, det_award_6, det_award_7, det_award_8])
    assert errors == 0


def test_failure(database):
    """
        Test failure for when AwardeeOrRecipientUEI is provided, it must be twelve characters.
        When AwardeeOrRecipientDUNS is provided, it must be nine digits.
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='00000000A', uei='123456789aBc',
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='ABCDEFGHI', uei='1234567s89aBc',
                                                          correction_delete_indicatr='c')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='AAA', uei='123456789aBc',
                                                          correction_delete_indicatr='C')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='AAAAAAAAAAA', uei='123456789aBc',
                                                          correction_delete_indicatr=None)
    det_award_5 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='123456789', uei='1',
                                                          correction_delete_indicatr=None)
    det_award_6 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='123456789', uei='123456789ABcd',
                                                          correction_delete_indicatr=None)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6])
    assert errors == 6
