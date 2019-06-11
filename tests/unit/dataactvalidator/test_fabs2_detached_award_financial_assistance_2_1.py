from tests.unit.dataactcore.factories.staging import (DetachedAwardFinancialAssistanceFactory,
                                                      PublishedAwardFinancialAssistanceFactory)
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs2_detached_award_financial_assistance_2_1'


def test_column_headers(database):
    expected_subset = {"row_number", "fain", "award_modification_amendme", "uri", "awarding_sub_tier_agency_c",
                       "correction_delete_indicatr"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ The combination of FAIN, AwardModificationAmendmentNumber, URI, and AwardingSubTierAgencyCode must be unique
        from currently published ones unless the record is a correction or deletion
        (i.e., if CorrectionDeleteIndicator = C or D). Ignores inactive records
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(afa_generated_unique="ama1asta1fain1uri1",
                                                          correction_delete_indicatr=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(afa_generated_unique="ama1asta1fain2uri1",
                                                          correction_delete_indicatr="C")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(afa_generated_unique="ama2asta1fain1uri1",
                                                          correction_delete_indicatr="D")
    pub_award_1 = PublishedAwardFinancialAssistanceFactory(afa_generated_unique="ama1asta1fain1uri2",
                                                           correction_delete_indicatr=None,
                                                           is_active=True)
    pub_award_2 = PublishedAwardFinancialAssistanceFactory(afa_generated_unique="ama1asta2fain1uri1",
                                                           correction_delete_indicatr=None,
                                                           is_active=True)
    pub_award_3 = PublishedAwardFinancialAssistanceFactory(afa_generated_unique="ama1asta1fain2uri1",
                                                           correction_delete_indicatr=None,
                                                           is_active=True)
    pub_award_4 = PublishedAwardFinancialAssistanceFactory(afa_generated_unique="ama2asta1fain1uri1",
                                                           correction_delete_indicatr=None,
                                                           is_active=True)
    pub_award_5 = PublishedAwardFinancialAssistanceFactory(afa_generated_unique="ama1asta1fain1uri1",
                                                           correction_delete_indicatr=None,
                                                           is_active=False)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, pub_award_1, pub_award_2,
                                                       pub_award_3, pub_award_4, pub_award_5])
    assert errors == 0


def test_failure(database):
    """ The combination of FAIN, AwardModificationAmendmentNumber, URI, and AwardingSubTierAgencyCode must be unique
        from currently published ones unless the record is a correction or deletion
        (i.e., if CorrectionDeleteIndicator = C or D).
    """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(afa_generated_unique="ama1asta1fain1uri1",
                                                          correction_delete_indicatr=None)
    # Test that capitalization differences don't affect the error
    det_award_2 = DetachedAwardFinancialAssistanceFactory(afa_generated_unique="amA1asta1faiN1uri1",
                                                          correction_delete_indicatr=None)
    pub_award_1 = PublishedAwardFinancialAssistanceFactory(afa_generated_unique="ama1asTa1fain1uri1",
                                                           correction_delete_indicatr=None,
                                                           is_active=True)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, pub_award_1])
    assert errors == 2
