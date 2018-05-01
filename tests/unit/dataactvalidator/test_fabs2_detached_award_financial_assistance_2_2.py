from tests.unit.dataactcore.factories.staging import (DetachedAwardFinancialAssistanceFactory,
                                                      PublishedAwardFinancialAssistanceFactory)
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs2_detached_award_financial_assistance_2_2'


def test_column_headers(database):
    expected_subset = {"row_number", "fain", "award_modification_amendme", "uri", "awarding_sub_tier_agency_c",
                       "correction_delete_indicatr"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ The unique combination of FAIN, AwardModificationAmendmentNumber, URI, and AwardingSubTierAgencyCode must exist
        as a currently published record when the record is a correction (i.e., if CorrectionDeleteIndicator = C).
        Ignore all other CorrectionDeleteIndicators in this rule. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(afa_generated_unique="ama1asta1fain1uri1",
                                                          correction_delete_indicatr=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(afa_generated_unique="ama1asta1fain2uri1",
                                                          correction_delete_indicatr="C")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(afa_generated_unique="ama2asta1fain1uri1",
                                                          correction_delete_indicatr="D")

    pub_award_1 = PublishedAwardFinancialAssistanceFactory(afa_generated_unique="ama1asta1fain2uri1",
                                                           correction_delete_indicatr=None,
                                                           is_active=True)
    pub_award_2 = PublishedAwardFinancialAssistanceFactory(afa_generated_unique="ama1asta1fain1uri1",
                                                           correction_delete_indicatr=None,
                                                           is_active=True)
    pub_award_3 = PublishedAwardFinancialAssistanceFactory(afa_generated_unique="ama2asta1fain1uri1",
                                                           correction_delete_indicatr=None,
                                                           is_active=False)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, pub_award_1, pub_award_2,
                                                       pub_award_3])
    assert errors == 0


def test_failure(database):
    """ The unique combination of FAIN, AwardModificationAmendmentNumber, URI, and AwardingSubTierAgencyCode must exist
        as a currently published record when the record is a correction (i.e., if CorrectionDeleteIndicator = C).
        Ignore all other CorrectionDeleteIndicators in this rule. """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(afa_generated_unique="ama1asta1fain2uri1",
                                                          correction_delete_indicatr="C")

    pub_award_1 = PublishedAwardFinancialAssistanceFactory(afa_generated_unique="ama1asta1fain1uri1",
                                                           correction_delete_indicatr=None,
                                                           is_active=True)
    pub_award_2 = PublishedAwardFinancialAssistanceFactory(afa_generated_unique="ama1asta1fain2uri1",
                                                           correction_delete_indicatr=None,
                                                           is_active=False)

    errors = number_of_errors(_FILE, database, models=[det_award_1, pub_award_1, pub_award_2])
    assert errors == 1
