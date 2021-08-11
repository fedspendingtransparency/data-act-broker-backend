from tests.unit.dataactcore.factories.staging import (
    DetachedAwardFinancialAssistanceFactory, PublishedAwardFinancialAssistanceFactory)
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs31_detached_award_financial_assistance_2_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'assistance_type', 'action_date', 'awardee_or_recipient_uniqu', 'uei',
                       'business_types', 'record_type', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """
        Test success for AwardeeOrRecipientDUNS or AwardeeOrRecipientUEI is required where ActionDate is after
        October 1, 2010, unless the record is an aggregate or PII-redacted non-aggregate record (RecordType = 1 or 3)
        or the recipient is an individual (BusinessTypes includes 'P'). For AssistanceType 06, 07, 08, 09, 10, or 11,
        if the base award (the earliest record with the same unique award key) has an ActionDate prior to April 4, 2022,
        this will produce a warning rather than a fatal error.
    """
    # Note: for FABS 31.1.2, we're setting assistance types to 06, 07, 08, 09, 10, or 11 and having the base
    #       actiondate be less than April 4, 2022. This rule will not trigger if those *do* apply.
    #       FABS 31.1.1 *will not* trigger when these apply.

    pub_award_1 = PublishedAwardFinancialAssistanceFactory(unique_award_key='before_key', action_date='20091001',
                                                           is_active=True)
    pub_award_2 = PublishedAwardFinancialAssistanceFactory(unique_award_key='after_key', action_date='20220404',
                                                           is_active=True)
    pub_award_3 = PublishedAwardFinancialAssistanceFactory(unique_award_key='inactive_key', action_date='20091001',
                                                           is_active=False)
    models = [pub_award_1, pub_award_2, pub_award_3]

    # new records that may or may not be related to older awards
    det_award_01 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='AbC',
                                                           awardee_or_recipient_uniqu='test', uei='test',
                                                           action_date='10/02/2010', assistance_type='06',
                                                           correction_delete_indicatr='',
                                                           unique_award_key='before_key')
    det_award_02 = DetachedAwardFinancialAssistanceFactory(record_type=5, business_types='aBc',
                                                           awardee_or_recipient_uniqu='', uei='test',
                                                           action_date='10/02/2010', assistance_type='07',
                                                           correction_delete_indicatr='c',
                                                           unique_award_key='before_key')
    det_award_03 = DetachedAwardFinancialAssistanceFactory(record_type=4, business_types='AbC',
                                                           awardee_or_recipient_uniqu='test', uei='',
                                                           action_date='10/02/2010', assistance_type='08',
                                                           correction_delete_indicatr='C',
                                                           unique_award_key='before_key')
    det_award_04 = DetachedAwardFinancialAssistanceFactory(record_type=5, business_types='aBc',
                                                           awardee_or_recipient_uniqu='test', uei=None,
                                                           action_date='10/02/2010', assistance_type='09',
                                                           correction_delete_indicatr=None,
                                                           unique_award_key='before_key')
    det_award_05 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='AbC',
                                                           awardee_or_recipient_uniqu=None, uei=None,
                                                           action_date='09/01/2010', assistance_type='10',
                                                           correction_delete_indicatr='',
                                                           unique_award_key='before_key')
    det_award_06 = DetachedAwardFinancialAssistanceFactory(record_type=5, business_types='aBc',
                                                           awardee_or_recipient_uniqu='', uei=None,
                                                           action_date='09/01/2010', assistance_type='11',
                                                           correction_delete_indicatr='',
                                                           unique_award_key='before_key')
    det_award_07 = DetachedAwardFinancialAssistanceFactory(record_type=4, business_types='AbC',
                                                           awardee_or_recipient_uniqu=None, uei='',
                                                           action_date='09/01/2010', assistance_type='06',
                                                           correction_delete_indicatr='',
                                                           unique_award_key='before_key')
    det_award_08 = DetachedAwardFinancialAssistanceFactory(record_type=5, business_types='AbC',
                                                           awardee_or_recipient_uniqu=None, uei=None,
                                                           action_date='09/01/2010', assistance_type='07',
                                                           correction_delete_indicatr='',
                                                           unique_award_key='before_key')
    det_award_09 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='AbC',
                                                           awardee_or_recipient_uniqu='test', uei='test',
                                                           action_date='09/01/2010', assistance_type='08',
                                                           correction_delete_indicatr='',
                                                           unique_award_key='before_key')
    det_award_10 = DetachedAwardFinancialAssistanceFactory(record_type=5, business_types='aBp',
                                                           awardee_or_recipient_uniqu=None, uei=None,
                                                           action_date='10/02/2010', assistance_type='09',
                                                           correction_delete_indicatr='',
                                                           unique_award_key='before_key')
    det_award_11 = DetachedAwardFinancialAssistanceFactory(record_type=1, business_types='AbC',
                                                           awardee_or_recipient_uniqu='', uei='',
                                                           action_date='04/05/2022', assistance_type='10',
                                                           correction_delete_indicatr='',
                                                           unique_award_key='inactive_key')
    det_award_12 = DetachedAwardFinancialAssistanceFactory(record_type=3, business_types='AbC',
                                                           awardee_or_recipient_uniqu='', uei='',
                                                           action_date='04/05/2022', assistance_type='11',
                                                           correction_delete_indicatr='',
                                                           unique_award_key='new_key')
    # Ignore correction delete indicator of D
    det_award_13 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='AbC',
                                                           awardee_or_recipient_uniqu=None, uei=None,
                                                           action_date='10/02/2010', assistance_type='06',
                                                           correction_delete_indicatr='d',
                                                           unique_award_key='before_key')
    # Ensuring that this rule gets ignored when the base actiondate case doesn't apply
    det_award_14 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='AbC',
                                                           awardee_or_recipient_uniqu='', uei='',
                                                           action_date='10/02/2010', assistance_type='02',
                                                           correction_delete_indicatr='',
                                                           unique_award_key='before_key')
    det_award_15 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='AbC',
                                                           awardee_or_recipient_uniqu='', uei='',
                                                           action_date='10/02/2010', assistance_type='06',
                                                           correction_delete_indicatr='',
                                                           unique_award_key='after_key')
    det_award_16 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='AbC',
                                                           awardee_or_recipient_uniqu='', uei='',
                                                           action_date='04/05/2022', assistance_type='06',
                                                           correction_delete_indicatr='',
                                                           unique_award_key='inactive_key')
    det_award_17 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='AbC',
                                                           awardee_or_recipient_uniqu='', uei='',
                                                           action_date='04/05/2022', assistance_type='06',
                                                           correction_delete_indicatr='',
                                                           unique_award_key='new_key')
    models += [det_award_01, det_award_02, det_award_03, det_award_04, det_award_05, det_award_06, det_award_07,
               det_award_08, det_award_09, det_award_10, det_award_11, det_award_12, det_award_13, det_award_14,
               det_award_15, det_award_16, det_award_17]

    errors = number_of_errors(_FILE, database, models=models)
    assert errors == 0


def test_failure(database):
    """
        Test failure for AwardeeOrRecipientDUNS or AwardeeOrRecipientUEI is required where ActionDate is after
        October 1, 2010, unless the record is an aggregate or PII-redacted non-aggregate record (RecordType = 1 or 3)
        or the recipient is an individual (BusinessTypes includes 'P'). For AssistanceType 06, 07, 08, 09, 10, or 11,
        if the base award (the earliest record with the same unique award key) has an ActionDate prior to April 4, 2022,
        this will produce a warning rather than a fatal error.
    """
    # Note: for FABS 31.1.2, we're setting assistance types to 06, 07, 08, 09, 10, or 11 and having the base
    #       actiondate be less than April 4, 2022. This rule will not trigger if those *do* apply.
    #       FABS 31.1.1 *will not* trigger when these apply.

    pub_award_1 = PublishedAwardFinancialAssistanceFactory(unique_award_key='before_key', action_date='20091001',
                                                           is_active=True)
    pub_award_2 = PublishedAwardFinancialAssistanceFactory(unique_award_key='after_key', action_date='20220404',
                                                           is_active=True)
    pub_award_3 = PublishedAwardFinancialAssistanceFactory(unique_award_key='inactive_key', action_date='20091001',
                                                           is_active=False)
    models = [pub_award_1, pub_award_2, pub_award_3]

    det_award_1 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='AbC',
                                                          awardee_or_recipient_uniqu=None, uei=None,
                                                          action_date='10/02/2010', assistance_type='06',
                                                          correction_delete_indicatr='',
                                                          unique_award_key='before_key')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=5, business_types='aBc',
                                                          awardee_or_recipient_uniqu='', uei=None,
                                                          action_date='10/02/2010', assistance_type='07',
                                                          correction_delete_indicatr='C',
                                                          unique_award_key='before_key')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(record_type=4, business_types='AbC',
                                                          awardee_or_recipient_uniqu=None, uei='',
                                                          action_date='10/02/2010', assistance_type='08',
                                                          correction_delete_indicatr='c',
                                                          unique_award_key='before_key')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(record_type=5, business_types='aBc',
                                                          awardee_or_recipient_uniqu='', uei='',
                                                          action_date='10/02/2010', assistance_type='09',
                                                          correction_delete_indicatr=None,
                                                          unique_award_key='before_key')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='AbC',
                                                          awardee_or_recipient_uniqu='', uei='',
                                                          action_date='10/02/2010', assistance_type='06',
                                                          correction_delete_indicatr='',
                                                          unique_award_key='inactive_key')
    det_award_6 = DetachedAwardFinancialAssistanceFactory(record_type=2, business_types='AbC',
                                                          awardee_or_recipient_uniqu='', uei='',
                                                          action_date='10/02/2010', assistance_type='06',
                                                          correction_delete_indicatr='',
                                                          unique_award_key='new_key')
    models += [det_award_1, det_award_2, det_award_3, det_award_4, det_award_5, det_award_6]

    errors = number_of_errors(_FILE, database, models=models)
    assert errors == 6
