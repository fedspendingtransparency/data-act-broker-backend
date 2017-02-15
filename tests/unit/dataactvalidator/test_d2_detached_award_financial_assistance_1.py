from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd2_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {"row_number", "fain", "award_modification_amendme", "uri", "awarding_sub_tier_agency_c"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests that all combinations of FAIN, AwardModificationAmendmentNumber, URI, and AwardingSubTierAgencyCode
        in File D2 (Detached Award Financial Assistance) are unique"""
    det_award_1 = DetachedAwardFinancialAssistanceFactory(fain="ABC", award_modification_amendme="ABC",
                                                          uri="ABC", awarding_sub_tier_agency_c="ABC",
                                                          correction_late_delete_ind=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(fain="ABCD", award_modification_amendme="ABC",
                                                          uri="ABC", awarding_sub_tier_agency_c="ABC",
                                                          correction_late_delete_ind=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(fain="ABC", award_modification_amendme="ABCD",
                                                          uri="ABC", awarding_sub_tier_agency_c="ABC",
                                                          correction_late_delete_ind=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(fain="ABC", award_modification_amendme="ABC",
                                                          uri="ABCD", awarding_sub_tier_agency_c="ABC",
                                                          correction_late_delete_ind=None)
    det_award_5 = DetachedAwardFinancialAssistanceFactory(fain="ABC", award_modification_amendme="ABC",
                                                          uri="ABC", awarding_sub_tier_agency_c="ABCD",
                                                          correction_late_delete_ind=None)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ Tests that all combinations of FAIN, AwardModificationAmendmentNumber, URI, and AwardingSubTierAgencyCode
        in File D2 (Detached Award Financial Assistance) are not unique"""

    det_award_1 = DetachedAwardFinancialAssistanceFactory(fain="ABC", award_modification_amendme="ABC",
                                                          uri="ABC", awarding_sub_tier_agency_c="ABC",
                                                          correction_late_delete_ind=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(fain="ABC", award_modification_amendme="ABC",
                                                          uri="ABC", awarding_sub_tier_agency_c="ABC",
                                                          correction_late_delete_ind=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(fain="ABC", award_modification_amendme="ABC",
                                                          uri="ABC", awarding_sub_tier_agency_c="ABC",
                                                          correction_late_delete_ind="L")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(fain="ABC", award_modification_amendme="ABC",
                                                          uri="ABC", awarding_sub_tier_agency_c="ABC",
                                                          correction_late_delete_ind="C")
    det_award_5 = DetachedAwardFinancialAssistanceFactory(fain="ABC", award_modification_amendme="ABC",
                                                          uri="ABC", awarding_sub_tier_agency_c="ABC",
                                                          correction_late_delete_ind="D")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 4
