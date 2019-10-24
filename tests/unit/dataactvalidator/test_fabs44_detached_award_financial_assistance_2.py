from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs44_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_zip5', 'legal_entity_zip_last4', 'legal_entity_congressional',
                       'legal_entity_country_code', 'record_type', 'uniqueid_afa_generated_unique'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test if LegalEntityZIPLast4 is not provided and LegalEntityZIP5 is, LegalEntityCongressionalDistrict must be
        provided for domestic and non-aggregate and PII-redacted non-aggregate records (RecordType = 2 or 3). This rule
        is ignored when CorrectionDeleteIndicator is D
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_zip_last4='6789',
                                                          legal_entity_congressional='01',
                                                          legal_entity_country_code='USA', record_type=2,
                                                          correction_delete_indicatr='C')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_zip_last4='6789',
                                                          legal_entity_congressional='',
                                                          legal_entity_country_code='usa', record_type=3,
                                                          correction_delete_indicatr='')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='', legal_entity_zip_last4='',
                                                          legal_entity_congressional='',
                                                          legal_entity_country_code='usa', record_type=2,
                                                          correction_delete_indicatr=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_zip_last4=None,
                                                          legal_entity_congressional='01',
                                                          legal_entity_country_code='USA', record_type=3,
                                                          correction_delete_indicatr='D')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_zip_last4='',
                                                          legal_entity_congressional='01',
                                                          legal_entity_country_code='USA', record_type=2,
                                                          correction_delete_indicatr='C')
    det_award_6 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_zip_last4='6789',
                                                          legal_entity_congressional=None,
                                                          legal_entity_country_code='usa', record_type=3,
                                                          correction_delete_indicatr='c')

    det_award_7 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_zip_last4=None,
                                                          legal_entity_congressional=None,
                                                          legal_entity_country_code='Another Country', record_type=2,
                                                          correction_delete_indicatr='')

    det_award_8 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_zip_last4='',
                                                          legal_entity_congressional='',
                                                          legal_entity_country_code='Not USA', record_type=3,
                                                          correction_delete_indicatr=None)

    det_award_9 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_zip_last4=None,
                                                          legal_entity_congressional=None,
                                                          legal_entity_country_code='USA', record_type=1,
                                                          correction_delete_indicatr='')

    # Ignored because of cdi of D
    det_award_10 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_zip_last4='',
                                                           legal_entity_congressional='',
                                                           legal_entity_country_code='USA', record_type=2,
                                                           correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, det_award_7, det_award_8, det_award_9,
                                                       det_award_10])
    assert errors == 0


def test_failure(database):
    """ Test failure if LegalEntityZIPLast4 is not provided and LegalEntityZIP5 is, LegalEntityCongressionalDistrict
        must be provided or domestic and non-aggregate and PII-redacted non-aggregate records (RecordType = 2 or 3)
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_zip_last4='',
                                                          legal_entity_congressional='',
                                                          legal_entity_country_code='USA', record_type=2,
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_zip_last4=None,
                                                          legal_entity_congressional='',
                                                          legal_entity_country_code='usa', record_type=3,
                                                          correction_delete_indicatr='C')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_zip_last4='',
                                                          legal_entity_congressional=None,
                                                          legal_entity_country_code='USA', record_type=2,
                                                          correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_zip_last4=None,
                                                          legal_entity_congressional=None,
                                                          legal_entity_country_code='usa', record_type=3,
                                                          correction_delete_indicatr=None)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 4
