from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory,PublishedFABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs3_detached_award_financial_assistance_3'


def test_column_headers(database):
    expected_subset = {'row_number', 'fain', 'uri', 'awarding_sub_tier_agency_c', 'action_type',
                       'correction_delete_indicatr', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ ActionType should be B, C, or D for transactions that modify existing awards.
        For aggregate (RecordType = 1) record transactions, we consider a record a modification if its combination of
        URI + AwardingSubTierAgencyCode matches an existing published FABS record of the same RecordType.
        For non-aggregate (RecordType = 2 or 3) transactions, we consider a record a modification if its combination
        of FAIN + AwardingSubTierCode matches those of an existing published non-aggregate FABS record
        (RecordType = 2 or 3) of the same RecordType.
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique1', action_type='B',
                                                          correction_delete_indicatr=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique2', action_type='d',
                                                          correction_delete_indicatr='')
    # Ignore delete/correction record
    det_award_3 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique2', action_type='A',
                                                          correction_delete_indicatr='D')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique2', action_type='A',
                                                          correction_delete_indicatr='c')
    # This is an inactive award so it will be ignored
    det_award_5 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique3', action_type='E',
                                                          correction_delete_indicatr=None)
    # This record doesn't have a matching published award at all
    det_award_6 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique4', action_type='A',
                                                          correction_delete_indicatr='')

    pub_fabs_1 = PublishedFABSFactory(unique_award_key='unique3', is_active=False)
    pub_fabs_2 = PublishedFABSFactory(unique_award_key='unique1', is_active=True)
    pub_fabs_3 = PublishedFABSFactory(unique_award_key='unique2', is_active=True)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, pub_fabs_1, pub_fabs_2, pub_fabs_3])
    assert errors == 0


def test_failure(database):
    """ Fail ActionType should be B, C, or D for transactions that modify existing awards. For aggregate
        (RecordType = 1) record transactions, we consider a record a modification if its combination of URI +
        AwardingSubTierAgencyCode matches an existing published FABS record of the same RecordType.
        For non-aggregate (RecordType = 2 or 3) transactions, we consider a record a modification if its combination
        of FAIN + AwardingSubTierCode matches those of an existing published non-aggregate FABS record
        (RecordType = 2 or 3) of the same RecordType.
    """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique1', action_type='a',
                                                          correction_delete_indicatr=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique1', action_type='E',
                                                          correction_delete_indicatr='')

    pub_fabs_1 = PublishedFABSFactory(unique_award_key='unique1', is_active=True)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, pub_fabs_1])
    assert errors == 2
