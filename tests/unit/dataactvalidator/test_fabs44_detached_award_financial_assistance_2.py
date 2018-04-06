from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs44_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "legal_entity_zip5", "legal_entity_zip_last4", "legal_entity_congressional"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test if LegalEntityZIPLast4 is not provided and LegalEntityZIP5 is, LegalEntityCongressionalDistrict must be
        provided. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="12345", legal_entity_zip_last4="6789",
                                                          legal_entity_congressional="01")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="12345", legal_entity_zip_last4="6789",
                                                          legal_entity_congressional="")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="", legal_entity_zip_last4="",
                                                          legal_entity_congressional="")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="12345", legal_entity_zip_last4=None,
                                                          legal_entity_congressional="01")
    det_award_5 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="12345", legal_entity_zip_last4="",
                                                          legal_entity_congressional="01")
    det_award_6 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="12345", legal_entity_zip_last4="6789",
                                                          legal_entity_congressional=None)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6])
    assert errors == 0


def test_failure(database):
    """ Test failure if LegalEntityZIPLast4 is not provided and LegalEntityZIP5 is, LegalEntityCongressionalDistrict
        must be provided. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="12345", legal_entity_zip_last4="",
                                                          legal_entity_congressional="")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="12345", legal_entity_zip_last4=None,
                                                          legal_entity_congressional="")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="12345", legal_entity_zip_last4="",
                                                          legal_entity_congressional=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="12345", legal_entity_zip_last4=None,
                                                          legal_entity_congressional=None)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 4
