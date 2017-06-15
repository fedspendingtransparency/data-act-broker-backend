from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd31_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "assistance_type", "action_date", "awardee_or_recipient_uniqu"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test success for AwardeeOrRecipientUniqueIdentifier is required for AssistanceType of 02, 03, 04, or 05 whose
    ActionDate after October 1, 2010. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(assistance_type="02", action_date="10/02/2010",
                                                          awardee_or_recipient_uniqu='test')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(assistance_type="03", action_date="09/01/2010",
                                                          awardee_or_recipient_uniqu='')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(assistance_type="06", action_date="10/02/2010",
                                                          awardee_or_recipient_uniqu='')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3])
    assert errors == 0


def test_failure(database):
    """ Test failure for AwardeeOrRecipientUniqueIdentifier is required for AssistanceType of 02, 03, 04, or 05 whose
    ActionDate after October 1, 2010. """

    det_award = DetachedAwardFinancialAssistanceFactory(assistance_type="02", action_date="10/02/2010",
                                                        awardee_or_recipient_uniqu='')

    errors = number_of_errors(_FILE, database, models=[det_award])
    assert errors == 1
