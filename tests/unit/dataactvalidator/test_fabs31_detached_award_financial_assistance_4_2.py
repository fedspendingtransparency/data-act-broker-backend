from tests.unit.dataactcore.factories.staging import (
    DetachedAwardFinancialAssistanceFactory, PublishedAwardFinancialAssistanceFactory)
from dataactcore.models.domainModels import DUNS
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs31_detached_award_financial_assistance_4_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'assistance_type', 'action_date', 'awardee_or_recipient_uniqu', 'uei',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_pubished_date_success(database):
    """
        Test success for when provided, AwardeeOrRecipientUEI and/or AwardeeOrRecipientDUNS must be registered
        (not necessarily active) in SAM, unless the ActionDate is before October 1, 2010.
        For AssistanceType 06, 07, 08, 09, 10, or 11 with an ActionDate prior to April 4, 2022, this will produce a
        warning rather than a fatal error.
    """
    # Note: for FABS 31.4.2, we're setting assistance types to 06, 07, 08, 09, 10, or 11 and having the base
    #       actiondate be less than April 4, 2022. This rule will not trigger if those *do* apply.
    #       FABS 31.4.1 *will not* trigger when these apply.

    pub_award_1 = PublishedAwardFinancialAssistanceFactory(unique_award_key='before_key', action_date='20091001',
                                                           is_active=True)
    pub_award_2 = PublishedAwardFinancialAssistanceFactory(unique_award_key='after_key', action_date='20220404',
                                                           is_active=True)
    pub_award_3 = PublishedAwardFinancialAssistanceFactory(unique_award_key='inactive_key', action_date='20091001',
                                                           is_active=False)
    models = [pub_award_1, pub_award_2, pub_award_3]

    # new records that may or may not be related to older awards
    duns_1 = DUNS(awardee_or_recipient_uniqu='111111111', uei=None)
    duns_2 = DUNS(awardee_or_recipient_uniqu=None, uei='22222222222E')
    duns_3 = DUNS(awardee_or_recipient_uniqu='333333333', uei='33333333333f')
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='111111111', uei=None,
                                                          assistance_type='06', action_date='10/02/2010',
                                                          correction_delete_indicatr='',
                                                          unique_award_key='before_key')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu=None, uei='22222222222e',
                                                          assistance_type='07', action_date='10/02/2010',
                                                          correction_delete_indicatr='c',
                                                          unique_award_key='before_key')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='333333333', uei='33333333333F',
                                                          assistance_type='08', action_date='10/02/2010',
                                                          correction_delete_indicatr='c',
                                                          unique_award_key='before_key')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='', uei=None,
                                                          assistance_type='09', action_date='10/02/2010',
                                                          correction_delete_indicatr='c',
                                                          unique_award_key='before_key')
    # Before October 1, 2010
    det_award_5 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='444444444', uei='',
                                                          assistance_type='10', action_date='09/30/2010',
                                                          correction_delete_indicatr='C',
                                                          unique_award_key='before_key')
    # Ignore correction delete indicator of D
    det_award_6 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='', uei='444444444444',
                                                          assistance_type='06', action_date='10/02/2010',
                                                          correction_delete_indicatr='d',
                                                          unique_award_key='before_key')
    # Ensuring that this rule gets ignored when the base actiondate case doesn't apply
    det_award_7 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='', uei='444444444444',
                                                          assistance_type='02', action_date='10/02/2010',
                                                          correction_delete_indicatr='',
                                                          unique_award_key='before_key')
    det_award_8 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='', uei='444444444444',
                                                          assistance_type='06', action_date='10/02/2010',
                                                          correction_delete_indicatr='',
                                                          unique_award_key='after_key')
    det_award_9 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='', uei='444444444444',
                                                          assistance_type='06', action_date='04/05/2022',
                                                          correction_delete_indicatr='',
                                                          unique_award_key='inactive_key')
    det_award_10 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='', uei='444444444444',
                                                           assistance_type='06', action_date='04/05/2022',
                                                           correction_delete_indicatr='',
                                                           unique_award_key='new_key')
    models += [duns_1, duns_2, duns_3, det_award_1, det_award_2, det_award_3, det_award_4, det_award_5, det_award_6,
               det_award_7, det_award_8, det_award_9, det_award_10]

    errors = number_of_errors(_FILE, database, models=models)
    assert errors == 0


def test_pubished_date_failure(database):
    """
        Test failure for when provided, AwardeeOrRecipientUEI and/or AwardeeOrRecipientDUNS must be registered
        (not necessarily active) in SAM, unless the ActionDate is before October 1, 2010.
        For AssistanceType 06, 07, 08, 09, 10, or 11 with an ActionDate prior to April 4, 2022, this will produce a
        warning rather than a fatal error.
    """
    # Note: for FABS 31.4.2, we're setting assistance types to 06, 07, 08, 09, 10, or 11 and having the base
    #       actiondate be less than April 4, 2022. This rule will not trigger if those *do* apply.
    #       FABS 31.4.1 *will not* trigger when these apply.

    pub_award_1 = PublishedAwardFinancialAssistanceFactory(unique_award_key='before_key', action_date='20091001',
                                                           is_active=True)
    pub_award_2 = PublishedAwardFinancialAssistanceFactory(unique_award_key='after_key', action_date='20220404',
                                                           is_active=True)
    pub_award_3 = PublishedAwardFinancialAssistanceFactory(unique_award_key='inactive_key', action_date='20091001',
                                                           is_active=False)
    models = [pub_award_1, pub_award_2, pub_award_3]

    # new records that may or may not be related to older awards
    duns_1 = DUNS(awardee_or_recipient_uniqu='111111111', uei=None)
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='444444444', uei='',
                                                          assistance_type='06', action_date='10/02/2010',
                                                          correction_delete_indicatr='',
                                                          unique_award_key='before_key')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='', uei='444444444444',
                                                          assistance_type='06', action_date='10/03/2010',
                                                          correction_delete_indicatr='c',
                                                          unique_award_key='before_key')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='444444444', uei='',
                                                          assistance_type='06', action_date='10/04/2010',
                                                          correction_delete_indicatr='C',
                                                          unique_award_key='before_key')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='', uei='444444444444',
                                                          assistance_type='06', action_date='10/05/2010',
                                                          correction_delete_indicatr=None,
                                                          unique_award_key='before_key')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='444444444', uei='',
                                                          assistance_type='06', action_date='10/04/2010',
                                                          correction_delete_indicatr='',
                                                          unique_award_key='inactive_key')
    det_award_6 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu='', uei='444444444444',
                                                          assistance_type='06', action_date='10/05/2010',
                                                          correction_delete_indicatr=None,
                                                          unique_award_key='new_key')
    models += [duns_1, det_award_1, det_award_2, det_award_3, det_award_4, det_award_5, det_award_6]

    errors = number_of_errors(_FILE, database, models=models)
    assert errors == 6
