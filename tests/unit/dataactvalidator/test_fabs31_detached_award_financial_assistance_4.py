from tests.unit.dataactcore.factories.staging import (
    DetachedAwardFinancialAssistanceFactory, PublishedAwardFinancialAssistanceFactory)
from dataactcore.models.domainModels import DUNS
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs31_detached_award_financial_assistance_4'


def test_column_headers(database):
    expected_subset = {'row_number', 'assistance_type', 'action_date', 'uei', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_pubished_date_success(database):
    """ Test success for when provided, AwardeeOrRecipientUEI must be registered (not necessarily active) in SAM,
        unless the ActionDate is before October 1, 2010.
    """

    pub_award_1 = PublishedAwardFinancialAssistanceFactory(unique_award_key='active_key', action_date='20091001',
                                                           is_active=True)
    pub_award_2 = PublishedAwardFinancialAssistanceFactory(unique_award_key='inactive_key', action_date='20091001',
                                                           is_active=False)
    models = [pub_award_1, pub_award_2]

    # new records that may or may not be related to older awards
    duns = DUNS(uei='22222222222E')
    det_award_1 = DetachedAwardFinancialAssistanceFactory(uei=duns.uei, assistance_type='02', action_date='10/02/2010',
                                                          correction_delete_indicatr='',
                                                          unique_award_key='active_key')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(uei=duns.uei.lower(), assistance_type='02',
                                                          action_date='10/02/2010', correction_delete_indicatr='c',
                                                          unique_award_key='active_key')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(uei=None, assistance_type='01', action_date='10/02/2010',
                                                          correction_delete_indicatr='c',
                                                          unique_award_key='active_key')
    # Before October 1, 2010
    det_award_4 = DetachedAwardFinancialAssistanceFactory(uei='12345', assistance_type='02', action_date='09/30/2010',
                                                          correction_delete_indicatr='C',
                                                          unique_award_key='new_key')
    # Ignore correction delete indicator of D
    det_award_5 = DetachedAwardFinancialAssistanceFactory(uei='12345', assistance_type='01', action_date='10/02/2010',
                                                          correction_delete_indicatr='d',
                                                          unique_award_key='inactive_key')

    models += [duns, det_award_1, det_award_2, det_award_3, det_award_4, det_award_5]

    errors = number_of_errors(_FILE, database, models=models)
    assert errors == 0


def test_pubished_date_failure(database):
    """ Test failure for when provided, AwardeeOrRecipientUEI must be registered (not necessarily active) in SAM,
        unless the ActionDate is before October 1, 2010.
    """

    pub_award_1 = PublishedAwardFinancialAssistanceFactory(unique_award_key='active_key', action_date='20220404',
                                                           is_active=True)
    pub_award_2 = PublishedAwardFinancialAssistanceFactory(unique_award_key='inactive_key', action_date='20091001',
                                                           is_active=False)
    models = [pub_award_1, pub_award_2]

    # new records that may or may not be related to older awards
    duns_1 = DUNS(uei='1111111111111E')
    det_award_1 = DetachedAwardFinancialAssistanceFactory(uei='12345', assistance_type='02', action_date='10/02/2010',
                                                          correction_delete_indicatr='',
                                                          unique_award_key='active_key')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(uei='12345', assistance_type='06', action_date='04/05/2022',
                                                          correction_delete_indicatr=None,
                                                          unique_award_key='inactive_key')
    models += [duns_1, det_award_1, det_award_2]

    errors = number_of_errors(_FILE, database, models=models)
    assert errors == 2
