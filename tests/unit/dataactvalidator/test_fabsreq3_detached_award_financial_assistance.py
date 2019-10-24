from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabsreq3_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'record_type', 'correction_delete_indicatr', 'uniqueid_afa_generated_unique'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test RecordType is required for all submissions except delete records. """

    det_award = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='C', record_type=1)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='', record_type=2)
    # Test ignoring for D records
    det_award_3 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='d', record_type=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='D', record_type=None)
    det_award_5 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='D', record_type=1)

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ Test fail RecordType is required for all submissions except delete records. """

    det_award = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='c', record_type=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr=None, record_type=None)

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
