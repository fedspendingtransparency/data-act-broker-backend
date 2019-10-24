from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabsreq7_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'assistance_type', 'correction_delete_indicatr', 'uniqueid_afa_generated_unique'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test AssistanceType is required for all submissions except delete records. """

    det_award = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='C', assistance_type='02')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='', assistance_type='05')
    # Test ignoring for D records
    det_award_3 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='d', assistance_type=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='D', assistance_type='')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='D', assistance_type='02')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ Test fail AssistanceType is required for all submissions except delete records. """

    det_award = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='c', assistance_type=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr=None, assistance_type='')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
