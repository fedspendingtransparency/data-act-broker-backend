from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import CFDAProgram
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs37_detached_award_financial_assistance_3'


def test_column_headers(database):
    expected_subset = {"row_number", "cfda_number"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test that no errors occur when the cfda_number exists. """

    cfda = CFDAProgram(program_number=12.340)
    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number="12.340", correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number="AB.CDE", correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, cfda])
    assert errors == 0


def test_failure(database):
    """ Test that its fails when cfda_number does not exists. """

    # test for cfda_number that doesn't exist in the table
    cfda = CFDAProgram(program_number=12.340)
    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number="54.321", correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number="AB.CDE", correction_delete_indicatr='c')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(cfda_number="11.111", correction_delete_indicatr=None)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, cfda])
    assert errors == 3
