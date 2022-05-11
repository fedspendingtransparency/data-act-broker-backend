from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactcore.factories.domain import ZipsFactory, StateCongressionalFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs44_detached_award_financial_assistance_3'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_zip5', 'legal_entity_congressional',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test If LegalEntityCongressionalDistrict is provided, it must be valid in the 5-digit zip code indicated by
        LegalEntityZIP5. Districts that were created under the 2000 census or later are considered valid. This rule is
        ignored when CorrectionDeleteIndicator is D
    """
    zip1 = ZipsFactory(zip5='12345', congressional_district_no='01', state_abbreviation='AB')
    zip2 = ZipsFactory(zip5='23456', congressional_district_no='01', state_abbreviation='CD')
    sc1 = StateCongressionalFactory(state_code='AB', congressional_district_no='01', census_year=None)
    sc2 = StateCongressionalFactory(state_code='CD', congressional_district_no='02', census_year=2000)

    fabs_1 = FABSFactory(legal_entity_zip5='12345', legal_entity_congressional='', correction_delete_indicatr='')
    fabs_2 = FABSFactory(legal_entity_zip5='', legal_entity_congressional='01', correction_delete_indicatr='C')
    fabs_3 = FABSFactory(legal_entity_zip5='', legal_entity_congressional='', correction_delete_indicatr='c')
    fabs_4 = FABSFactory(legal_entity_zip5='', legal_entity_congressional='', correction_delete_indicatr='d')
    fabs_5 = FABSFactory(legal_entity_zip5='12345', legal_entity_congressional=None, correction_delete_indicatr=None)
    fabs_6 = FABSFactory(legal_entity_zip5='12345', legal_entity_congressional='01', correction_delete_indicatr='c')
    fabs_7 = FABSFactory(legal_entity_zip5='23456', legal_entity_congressional='02', correction_delete_indicatr='')

    # Test ignore cdi of D
    fabs_8 = FABSFactory(legal_entity_zip5='12345', legal_entity_congressional='03', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7, fabs_8,
                                                       zip1, zip2, sc1, sc2])
    assert errors == 0


def test_failure(database):
    """ Test failure If LegalEntityCongressionalDistrict is provided, it must be valid in the 5-digit zip code indicated
        by LegalEntityZIP5. Districts that were created under the 2000 census or later are considered valid
    """
    zip1 = ZipsFactory(zip5='12345', congressional_district_no='01', state_abbreviation='AB')
    sc1 = StateCongressionalFactory(state_code='AB', congressional_district_no='01', census_year=None)
    sc2 = StateCongressionalFactory(state_code='AB', congressional_district_no='02', census_year=1999)

    fabs_1 = FABSFactory(legal_entity_zip5='12345', legal_entity_congressional='02', correction_delete_indicatr='')
    fabs_2 = FABSFactory(legal_entity_zip5='12346', legal_entity_congressional='01', correction_delete_indicatr=None)
    fabs_3 = FABSFactory(legal_entity_zip5='12345', legal_entity_congressional='03', correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, zip1, sc1, sc2])
    assert errors == 3
