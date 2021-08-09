from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import DUNS
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs31_detached_award_financial_assistance_7'


def test_column_headers(database):
    expected_subset = {'row_number', 'assistance_type', 'action_date', 'action_type', 'awardee_or_recipient_uniqu',
                       'uei', 'business_types', 'record_type', 'federal_action_obligation',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_pubished_date_success(database):
    """
        Test success for when ActionDate is after October 1, 2010 and ActionType = B, C, or D, AwardeeOrRecipientUEI
        and AwardeeOrRecipientDUNS should (when provided) have an active registration in SAM as of the ActionDate,
        except where FederalActionObligation is <=0 and ActionType = D.
    """
    duns_1 = DUNS(awardee_or_recipient_uniqu='111111111', uei='111111111111', registration_date='01/01/2017',
                  expiration_date='01/01/2018')
    duns_2 = DUNS(awardee_or_recipient_uniqu='222222222', uei='222222222222', registration_date='01/01/2018',
                  expiration_date='01/01/2019')
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='111111111', uei=None,
                                                          action_type='b', action_date='06/22/2017',
                                                          federal_action_obligation=10,
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='', uei='222222222222',
                                                          action_type='c', action_date='06/22/2018',
                                                          federal_action_obligation=10,
                                                          correction_delete_indicatr='')
    # Note: if both duns and uei are both provided, it will check against duns only
    #       they _should_ both point to the same recipient and FABS 31.8 will enforce this
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='111111111', uei='222222222222',
                                                          action_type='D', action_date='06/22/2017',
                                                          federal_action_obligation=10,
                                                          correction_delete_indicatr='')
    # Ignore different action type
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='111111111', uei='111111111111',
                                                          action_type='a', action_date='06/20/2017',
                                                          federal_action_obligation=10,
                                                          correction_delete_indicatr='')
    # Ignore FOA <= 0 and ActionType D
    det_award_5 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='111111111', uei='111111111111',
                                                          action_type='d', action_date='06/20/2017',
                                                          federal_action_obligation=-10,
                                                          correction_delete_indicatr='')
    # Ignore Before October 1, 2010
    det_award_6 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='111111111', uei='111111111111',
                                                          action_type='B', action_date='09/30/2010',
                                                          federal_action_obligation=10,
                                                          correction_delete_indicatr=None)
    # Ignore correction delete indicator of D
    det_award_7 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='111111111', uei='111111111111',
                                                          action_type='c', action_date='06/20/2020',
                                                          federal_action_obligation=10,
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[duns_1, duns_2, det_award_1, det_award_2, det_award_3,
                                                       det_award_4, det_award_5, det_award_6, det_award_7])
    assert errors == 0


def test_pubished_date_failure(database):
    """
        Test success for when ActionDate is after October 1, 2010 and ActionType = B, C, or D, AwardeeOrRecipientUEI
        and AwardeeOrRecipientDUNS should (when provided) have an active registration in SAM as of the ActionDate,
        except where FederalActionObligation is <=0 and ActionType = D.
    """
    duns_1 = DUNS(awardee_or_recipient_uniqu='111111111', uei='111111111111', registration_date='01/01/2017',
                  expiration_date='01/01/2018')
    duns_2 = DUNS(awardee_or_recipient_uniqu='222222222', uei='222222222222', registration_date='01/01/2018',
                  expiration_date='01/01/2019')
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='111111111', uei='',
                                                          action_type='b', action_date='06/20/2020',
                                                          federal_action_obligation=10,
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu=None, uei='111111111111',
                                                          action_type='C', action_date='06/20/2020',
                                                          federal_action_obligation=10,
                                                          correction_delete_indicatr='')
    # Note: if both duns and uei are both provided, it will check against duns only
    #       they _should_ both point to the same recipient and FABS 31.8 will enforce this
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='111111111111', uei='222222222222',
                                                          action_type='D', action_date='06/20/2018',
                                                          federal_action_obligation=10,
                                                          correction_delete_indicatr='')
    # FOA <= 0 and ActionType D checks individually
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='111111111', uei='111111111111',
                                                          action_type='d', action_date='06/20/2020',
                                                          federal_action_obligation=1,
                                                          correction_delete_indicatr='')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='111111111', uei='111111111111',
                                                          action_type='b', action_date='06/20/2020',
                                                          federal_action_obligation=0,
                                                          correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[duns_1, duns_2, det_award_1, det_award_2, det_award_3,
                                                       det_award_4, det_award_5])
    assert errors == 5
