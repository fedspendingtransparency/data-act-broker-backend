from tests.unit.dataactcore.factories.staging import (DetachedAwardFinancialAssistanceFactory,
                                                      PublishedAwardFinancialAssistanceFactory)
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs3_detached_award_financial_assistance_3'


def test_column_headers(database):
    expected_subset = {'row_number', 'fain', 'uri', 'awarding_sub_tier_agency_c', 'action_type',
                       'correction_delete_indicatr', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ ActionType should be B, C, or D for transactions that modify existing awards. For aggregate (RecordType = 1)
        record transactions, we consider a record a modification if its combination of URI + AwardingSubTierAgencyCode
        matches an existing published FABS record of the same RecordType. For non-aggregate (RecordType = 2 or 3) record
        transactions, we consider a record a modification if its combination of FAIN + AwardingSubTierCode matches those
        of an existing published non-aggregate FABS record (RecordType = 2 or 3). This validation rule does not apply to
        delete records (CorrectionDeleteIndicator = D.)
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique1', action_type='B',
                                                          correction_delete_indicatr=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique2', action_type='d',
                                                          correction_delete_indicatr='C')
    # Ignore delete record
    det_award_3 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique2', action_type='A',
                                                          correction_delete_indicatr='D')
    # This is an inactive award so it will be ignored
    det_award_4 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique3', action_type='E',
                                                          correction_delete_indicatr=None)
    # This record doesn't have a matching published award at all
    det_award_5 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique4', action_type='A',
                                                          correction_delete_indicatr='')

    pub_award_1 = PublishedAwardFinancialAssistanceFactory(unique_award_key='unique3', is_active=False)
    pub_award_2 = PublishedAwardFinancialAssistanceFactory(unique_award_key='unique1', is_active=True)
    pub_award_3 = PublishedAwardFinancialAssistanceFactory(unique_award_key='unique2', is_active=True)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       pub_award_1, pub_award_2, pub_award_3])
    assert errors == 0


def test_failure(database):
    """ Fail ActionType should be B, C, or D for transactions that modify existing awards. For aggregate
        (RecordType = 1) record transactions, we consider a record a modification if its combination of URI +
        AwardingSubTierAgencyCode matches an existing published FABS record of the same RecordType. For non-aggregate
        (RecordType = 2 or 3) record transactions, we consider a record a modification if its combination of FAIN +
        AwardingSubTierCode matches those of an existing published non-aggregate FABS record (RecordType = 2 or 3).
        This validation rule does not apply to delete records (CorrectionDeleteIndicator = D.)
    """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique1', action_type='a',
                                                          correction_delete_indicatr='c')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique1', action_type='E',
                                                          correction_delete_indicatr='')

    pub_award_1 = PublishedAwardFinancialAssistanceFactory(unique_award_key='unique1', is_active=True)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, pub_award_1])
    assert errors == 2
