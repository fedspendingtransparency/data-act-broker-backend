from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs26_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'assistance_type', 'federal_action_obligation', 'uniqueid_afa_generated_unique'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ FederalActionObligation is required for non-loans (i.e., when AssistanceType is not 07 or 08). """

    det_award = DetachedAwardFinancialAssistanceFactory(assistance_type='02', federal_action_obligation=0,
                                                        correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(assistance_type='10', federal_action_obligation=20,
                                                          correction_delete_indicatr='c')
    # Ignore correction delete indicator of D
    det_award_3 = DetachedAwardFinancialAssistanceFactory(assistance_type='03', federal_action_obligation=None,
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3])
    assert errors == 0


def test_failure(database):
    """ FederalActionObligation is required for non-loans (i.e., when AssistanceType is not 07 or 08). """

    det_award = DetachedAwardFinancialAssistanceFactory(assistance_type='03', federal_action_obligation=None,
                                                        correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[det_award])
    assert errors == 1
