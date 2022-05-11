from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs44_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_country_code', 'legal_entity_congressional',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test when LegalEntityCountryCode is not USA, LegalEntityCongressionalDistrict must be blank. This rule is
        ignored when CorrectionDeleteIndicator is D.
    """
    fabs_1 = FABSFactory(legal_entity_country_code='USA', legal_entity_congressional=None,
                         correction_delete_indicatr='C')
    fabs_2 = FABSFactory(legal_entity_country_code='usA', legal_entity_congressional='',
                         correction_delete_indicatr=None)
    fabs_3 = FABSFactory(legal_entity_country_code='USA', legal_entity_congressional='01',
                         correction_delete_indicatr='D')
    fabs_4 = FABSFactory(legal_entity_country_code='ABc', legal_entity_congressional='', correction_delete_indicatr='')
    fabs_5 = FABSFactory(legal_entity_country_code='', legal_entity_congressional=None, correction_delete_indicatr='C')
    fabs_6 = FABSFactory(legal_entity_country_code='ABC', legal_entity_congressional='01',
                         correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6])
    assert errors == 0


def test_failure(database):
    """ Test failure when LegalEntityCountryCode is not USA, LegalEntityCongressionalDistrict must be blank. """
    fabs_1 = FABSFactory(legal_entity_country_code='AbC', legal_entity_congressional='02',
                         correction_delete_indicatr='C')
    fabs_2 = FABSFactory(legal_entity_country_code='', legal_entity_congressional='42', correction_delete_indicatr=None)

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2])
    assert errors == 2
