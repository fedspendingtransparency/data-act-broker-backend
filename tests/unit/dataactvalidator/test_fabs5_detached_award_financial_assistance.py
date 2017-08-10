from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs5_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "assistance_type"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests success for when AssistanceType field is required and must be one of the allowed values:
        '02', '03', '04', '05', '06', '07', '08', '09', '10', '11' """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(assistance_type="02")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(assistance_type="03")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(assistance_type="04")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(assistance_type="05")
    det_award_5 = DetachedAwardFinancialAssistanceFactory(assistance_type="06")
    det_award_6 = DetachedAwardFinancialAssistanceFactory(assistance_type="07")
    det_award_7 = DetachedAwardFinancialAssistanceFactory(assistance_type="08")
    det_award_8 = DetachedAwardFinancialAssistanceFactory(assistance_type="09")
    det_award_9 = DetachedAwardFinancialAssistanceFactory(assistance_type="10")
    det_award_10 = DetachedAwardFinancialAssistanceFactory(assistance_type="11")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, det_award_7, det_award_8, det_award_9,
                                                       det_award_10])
    assert errors == 0


def test_failure(database):
    """ Tests failure for when AssistanceType field is required and must be one of the allowed values:
    '02', '03', '04', '05', '06', '07', '08', '09', '10', '11' """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(assistance_type="")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(assistance_type=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(assistance_type="random")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3])
    assert errors == 3
