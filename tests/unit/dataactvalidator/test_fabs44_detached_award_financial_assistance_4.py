from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'fabs44_detached_award_financial_assistance_4'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_congressional', 'record_type', 'uniqueid_afa_generated_unique'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test LegalEntityCongressionalDistrict must be blank for aggregate records (RecordType = 1). """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_congressional=None, record_type=1,
                                                          correction_delete_indicatr='C')

    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_congressional='', record_type=1,
                                                          correction_delete_indicatr=None)

    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_congressional='01', record_type=2,
                                                          correction_delete_indicatr=None)

    # Ignoring for correction_delete_indicator of D
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_congressional='', record_type=1,
                                                          correction_delete_indicatr='D')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ Test failure LegalEntityCongressionalDistrict must be blank for aggregate records (RecordType = 1). """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_congressional='01', record_type=1,
                                                          correction_delete_indicatr=None)

    errors = number_of_errors(_FILE, database, models=[det_award_1])
    assert errors == 1
