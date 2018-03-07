from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactcore.factories.domain import ZipsFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs44_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "legal_entity_zip5", "legal_entity_congressional"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test If LegalEntityCongressionalDistrict is provided, it must be valid in the 5-digit zip code indicated by
        LegalEntityZIP5. """
    zip1 = ZipsFactory(zip5="12345", congressional_district_no="01")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="12345", legal_entity_congressional="")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="", legal_entity_congressional="01")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="", legal_entity_congressional="")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="12345", legal_entity_congressional=None)
    det_award_5 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="12345", legal_entity_congressional="01")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       zip1])
    assert errors == 0


def test_failure(database):
    """ Test failure If LegalEntityCongressionalDistrict is provided, it must be valid in the 5-digit zip code indicated
        by LegalEntityZIP5. """
    zip1 = ZipsFactory(zip5="12345", congressional_district_no="01")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="12345", legal_entity_congressional="02")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5="12346", legal_entity_congressional="01")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, zip1])
    assert errors == 2
