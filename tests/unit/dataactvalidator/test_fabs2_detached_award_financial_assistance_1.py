from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs2_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'fain', 'award_modification_amendme', 'uri', 'awarding_sub_tier_agency_c',
                       'cfda_number', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests that all combinations of FAIN, AwardModificationAmendmentNumber, URI, CFDA_Number, and
        AwardingSubTierAgencyCode in File D2 (Detached Award Financial Assistance) are unique
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(afa_generated_unique='abc_def_ghi',
                                                          correction_delete_indicatr=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(afa_generated_unique='abc_def_ghij',
                                                          correction_delete_indicatr='')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(afa_generated_unique='abcd_efg_hij',
                                                          correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3])
    assert errors == 0


def test_failure(database):
    """ Tests that all combinations of FAIN, AwardModificationAmendmentNumber, URI, CFDA_Number, and
        AwardingSubTierAgencyCode in File D2 (Detached Award Financial Assistance) are not unique. Make sure casing is
        ignored.
    """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(afa_generated_unique='abc_def_ghi',
                                                          correction_delete_indicatr=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(afa_generated_unique='aBC_def_ghi',
                                                          correction_delete_indicatr='C')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(afa_generated_unique='abc_deF_ghi',
                                                          correction_delete_indicatr='D')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(afa_generated_unique='abc_def_GHI',
                                                          correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 3
