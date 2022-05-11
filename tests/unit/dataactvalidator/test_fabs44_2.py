from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs44_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_zip5', 'legal_entity_zip_last4', 'legal_entity_congressional',
                       'legal_entity_country_code', 'record_type', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test if LegalEntityZIPLast4 is not provided and LegalEntityZIP5 is, LegalEntityCongressionalDistrict must be
        provided for domestic and non-aggregate and PII-redacted non-aggregate records (RecordType = 2 or 3). This rule
        is ignored when CorrectionDeleteIndicator is D
    """
    fabs_1 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='6789', legal_entity_congressional='01',
                         legal_entity_country_code='USA', record_type=2, correction_delete_indicatr='C')
    fabs_2 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='6789', legal_entity_congressional='',
                         legal_entity_country_code='usa', record_type=3, correction_delete_indicatr='')
    fabs_3 = FABSFactory(legal_entity_zip5='', legal_entity_zip_last4='', legal_entity_congressional='',
                         legal_entity_country_code='usa', record_type=2, correction_delete_indicatr=None)
    fabs_4 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4=None, legal_entity_congressional='01',
                         legal_entity_country_code='USA', record_type=3, correction_delete_indicatr='D')
    fabs_5 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='', legal_entity_congressional='01',
                         legal_entity_country_code='USA', record_type=2, correction_delete_indicatr='C')
    fabs_6 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='6789', legal_entity_congressional=None,
                         legal_entity_country_code='usa', record_type=3, correction_delete_indicatr='c')
    fabs_7 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4=None, legal_entity_congressional=None,
                         legal_entity_country_code='Another Country', record_type=2, correction_delete_indicatr='')
    fabs_8 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='', legal_entity_congressional='',
                         legal_entity_country_code='Not USA', record_type=3, correction_delete_indicatr=None)
    fabs_9 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4=None, legal_entity_congressional=None,
                         legal_entity_country_code='USA', record_type=1, correction_delete_indicatr='')

    # Ignored because of cdi of D
    fabs_10 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='', legal_entity_congressional='', legal_entity_country_code='USA', record_type=2, correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7, fabs_8,
                                                       fabs_9, fabs_10])
    assert errors == 0


def test_failure(database):
    """ Test failure if LegalEntityZIPLast4 is not provided and LegalEntityZIP5 is, LegalEntityCongressionalDistrict
        must be provided or domestic and non-aggregate and PII-redacted non-aggregate records (RecordType = 2 or 3)
    """
    fabs_1 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='', legal_entity_congressional='',
                         legal_entity_country_code='USA', record_type=2, correction_delete_indicatr='')
    fabs_2 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4=None, legal_entity_congressional='',
                         legal_entity_country_code='usa', record_type=3, correction_delete_indicatr='C')
    fabs_3 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='', legal_entity_congressional=None,
                         legal_entity_country_code='USA', record_type=2, correction_delete_indicatr='c')
    fabs_4 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4=None, legal_entity_congressional=None,
                         legal_entity_country_code='usa', record_type=3, correction_delete_indicatr=None)

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 4
