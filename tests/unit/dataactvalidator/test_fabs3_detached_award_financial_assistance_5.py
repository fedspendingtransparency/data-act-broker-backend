from tests.unit.dataactcore.factories.staging import (DetachedAwardFinancialAssistanceFactory,
                                                      PublishedAwardFinancialAssistanceFactory)
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs3_detached_award_financial_assistance_5'


def test_column_headers(database):
    expected_subset = {'row_number', 'fain', 'uri', 'awarding_sub_tier_agency_c', 'action_type', 'record_type',
                       'correction_delete_indicatr', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ ActionType should be "A" for the initial transaction of a new, non-aggregate award (RecordType = 2 or 3) and "A"
        or "E" for a new aggregate award (RecordType = 1). An aggregate record transaction is considered the initial
        transaction of a new award if it contains a unique combination of URI + AwardingSubTierAgencyCode when compared
        to currently published FABS data of the same RecordType. A non-aggregate record transaction is considered the
        initial transaction of a new award if it contains a unique combination of FAIN + AwardingSubTierAgencyCode when
        compared to currently published FABS data of the same RecordType. This validation rule does not apply to delete
        records (CorrectionDeleteIndicator = D.)
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique1', action_type='A', record_type=1,
                                                          correction_delete_indicatr=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique1', action_type='e', record_type=1,
                                                          correction_delete_indicatr='C')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique2', action_type='a', record_type=3,
                                                          correction_delete_indicatr='C')
    # Ignore delete record
    det_award_4 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique1', action_type='C', record_type=3,
                                                          correction_delete_indicatr='D')
    # This is an active award so it will be ignored
    det_award_5 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique3', action_type='d', record_type=2,
                                                          correction_delete_indicatr=None)
    det_award_6 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique3', action_type='e', record_type=2,
                                                          correction_delete_indicatr=None)

    pub_award_1 = PublishedAwardFinancialAssistanceFactory(unique_award_key='unique2', is_active=False)
    pub_award_2 = PublishedAwardFinancialAssistanceFactory(unique_award_key='unique3', is_active=True)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, pub_award_1, pub_award_2])
    assert errors == 0


def test_failure(database):
    """ Fail ActionType should be "A" for the initial transaction of a new, non-aggregate award (RecordType = 2 or 3)
        and "A" or "E" for a new aggregate award (RecordType = 1). An aggregate record transaction is considered the
        initial transaction of a new award if it contains a unique combination of URI + AwardingSubTierAgencyCode when
        compared to currently published FABS data of the same RecordType. A non-aggregate record transaction is
        considered the initial transaction of a new award if it contains a unique combination of FAIN +
        AwardingSubTierAgencyCode when compared to currently published FABS data of the same RecordType. This validation
        rule does not apply to delete records (CorrectionDeleteIndicator = D.)
    """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique1', action_type='b', record_type=1,
                                                          correction_delete_indicatr='c')
    # E is only valid for record type 1
    det_award_2 = DetachedAwardFinancialAssistanceFactory(unique_award_key='unique2', action_type='E', record_type=2,
                                                          correction_delete_indicatr='')

    pub_award_1 = PublishedAwardFinancialAssistanceFactory(unique_award_key='unique2', is_active=False)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, pub_award_1])
    assert errors == 2
