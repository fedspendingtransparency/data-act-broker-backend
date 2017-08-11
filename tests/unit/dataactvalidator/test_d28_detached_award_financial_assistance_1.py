from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd28_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {"row_number", "assistance_type", "face_value_loan_guarantee"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ FaceValueLoanGuarantee is required for loans (i.e., when AssistanceType = 07 or 08). """

    det_award = DetachedAwardFinancialAssistanceFactory(assistance_type="07", face_value_loan_guarantee=0)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(assistance_type="08", face_value_loan_guarantee=20)

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 0


def test_failure(database):
    """ FaceValueLoanGuarantee is required for loans (i.e., when AssistanceType = 07 or 08). """

    det_award = DetachedAwardFinancialAssistanceFactory(assistance_type="07", face_value_loan_guarantee=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(assistance_type="08", face_value_loan_guarantee=None)

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
