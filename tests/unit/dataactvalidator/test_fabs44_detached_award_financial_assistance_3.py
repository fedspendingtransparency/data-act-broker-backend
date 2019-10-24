from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactcore.factories.domain import ZipsFactory, StateCongressionalFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs44_detached_award_financial_assistance_3'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_zip5', 'legal_entity_congressional', 'uniqueid_afa_generated_unique'}
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

    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_congressional='',
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='', legal_entity_congressional='01',
                                                          correction_delete_indicatr='C')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='', legal_entity_congressional='',
                                                          correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='', legal_entity_congressional='',
                                                          correction_delete_indicatr='d')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_congressional=None,
                                                          correction_delete_indicatr=None)
    det_award_6 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_congressional='01',
                                                          correction_delete_indicatr='c')
    det_award_7 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='23456', legal_entity_congressional='02',
                                                          correction_delete_indicatr='')
    # Test ignore cdi of D
    det_award_8 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_congressional='03',
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, det_award_7, det_award_8, zip1, zip2, sc1, sc2])
    assert errors == 0


def test_failure(database):
    """ Test failure If LegalEntityCongressionalDistrict is provided, it must be valid in the 5-digit zip code indicated
        by LegalEntityZIP5. Districts that were created under the 2000 census or later are considered valid
    """
    zip1 = ZipsFactory(zip5='12345', congressional_district_no='01', state_abbreviation='AB')
    sc1 = StateCongressionalFactory(state_code='AB', congressional_district_no='01', census_year=None)
    sc2 = StateCongressionalFactory(state_code='AB', congressional_district_no='02', census_year=1999)

    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_congressional='02',
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12346', legal_entity_congressional='01',
                                                          correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip5='12345', legal_entity_congressional='03',
                                                          correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, zip1, sc1, sc2])
    assert errors == 3
