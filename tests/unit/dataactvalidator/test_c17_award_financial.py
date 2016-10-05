from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from random import randint

_FILE = 'c17_award_financial'

def test_column_headers(database):
    expected_subset = {'row_number', 'transaction_obligated_amou'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test cases with amount present and with at least one ussgl field populated """

    # Test with only one present
    award_fin_amount = AwardFinancialFactory(transaction_obligated_amou = randint(1,1000),
                                             ussgl480100_undelivered_or_cpe = None,
                                             ussgl480100_undelivered_or_fyb = None,
		                                     ussgl480200_undelivered_or_cpe = None,
                                             ussgl480200_undelivered_or_fyb = None,
                                             ussgl483100_undelivered_or_cpe = None,
                                             ussgl483200_undelivered_or_cpe = None,
                                             ussgl487100_downward_adjus_cpe = None,
                                             ussgl487200_downward_adjus_cpe = None,
                                             ussgl488100_upward_adjustm_cpe = None,
                                             ussgl488200_upward_adjustm_cpe = None,
                                             ussgl490100_delivered_orde_cpe = None,
                                             ussgl490100_delivered_orde_fyb = None,
                                             ussgl490200_delivered_orde_cpe = None,
                                             ussgl490800_authority_outl_cpe = None,
                                             ussgl490800_authority_outl_fyb = None,
                                             ussgl493100_delivered_orde_cpe = None,
                                             ussgl497100_downward_adjus_cpe = None,
                                             ussgl497200_downward_adjus_cpe = None,
                                             ussgl498100_upward_adjustm_cpe = None,
                                             ussgl498200_upward_adjustm_cpe = None)
    award_fin_one = AwardFinancialFactory(transaction_obligated_amou = 0,
                                             ussgl480100_undelivered_or_cpe = None,
                                             ussgl480100_undelivered_or_fyb = None,
		                                     ussgl480200_undelivered_or_cpe = None,
                                             ussgl480200_undelivered_or_fyb = None,
                                             ussgl483100_undelivered_or_cpe = None,
                                             ussgl483200_undelivered_or_cpe = randint(1,1000),
                                             ussgl487100_downward_adjus_cpe = None,
                                             ussgl487200_downward_adjus_cpe = None,
                                             ussgl488100_upward_adjustm_cpe = None,
                                             ussgl488200_upward_adjustm_cpe = None,
                                             ussgl490100_delivered_orde_cpe = None,
                                             ussgl490100_delivered_orde_fyb = None,
                                             ussgl490200_delivered_orde_cpe = None,
                                             ussgl490800_authority_outl_cpe = None,
                                             ussgl490800_authority_outl_fyb = None,
                                             ussgl493100_delivered_orde_cpe = None,
                                             ussgl497100_downward_adjus_cpe = None,
                                             ussgl497200_downward_adjus_cpe = None,
                                             ussgl498100_upward_adjustm_cpe = None,
                                             ussgl498200_upward_adjustm_cpe = None)
    award_fin_two = AwardFinancialFactory(transaction_obligated_amou = None,
                                             ussgl480100_undelivered_or_cpe = None,
                                             ussgl480100_undelivered_or_fyb = None,
		                                     ussgl480200_undelivered_or_cpe = None,
                                             ussgl480200_undelivered_or_fyb = None,
                                             ussgl483100_undelivered_or_cpe = None,
                                             ussgl483200_undelivered_or_cpe = None,
                                             ussgl487100_downward_adjus_cpe = None,
                                             ussgl487200_downward_adjus_cpe = None,
                                             ussgl488100_upward_adjustm_cpe = None,
                                             ussgl488200_upward_adjustm_cpe = None,
                                             ussgl490100_delivered_orde_cpe = None,
                                             ussgl490100_delivered_orde_fyb = None,
                                             ussgl490200_delivered_orde_cpe = None,
                                             ussgl490800_authority_outl_cpe = None,
                                             ussgl490800_authority_outl_fyb = randint(1,1000),
                                             ussgl493100_delivered_orde_cpe = None,
                                             ussgl497100_downward_adjus_cpe = None,
                                             ussgl497200_downward_adjus_cpe = None,
                                             ussgl498100_upward_adjustm_cpe = None,
                                             ussgl498200_upward_adjustm_cpe = None)
    award_fin_three = AwardFinancialFactory(transaction_obligated_amou = None,
                                             ussgl480100_undelivered_or_cpe = None,
                                             ussgl480100_undelivered_or_fyb = None,
		                                     ussgl480200_undelivered_or_cpe = None,
                                             ussgl480200_undelivered_or_fyb = None,
                                             ussgl483100_undelivered_or_cpe = None,
                                             ussgl483200_undelivered_or_cpe = randint(1,1000),
                                             ussgl487100_downward_adjus_cpe = None,
                                             ussgl487200_downward_adjus_cpe = None,
                                             ussgl488100_upward_adjustm_cpe = None,
                                             ussgl488200_upward_adjustm_cpe = None,
                                             ussgl490100_delivered_orde_cpe = randint(1,1000),
                                             ussgl490100_delivered_orde_fyb = None,
                                             ussgl490200_delivered_orde_cpe = None,
                                             ussgl490800_authority_outl_cpe = None,
                                             ussgl490800_authority_outl_fyb = None,
                                             ussgl493100_delivered_orde_cpe = None,
                                             ussgl497100_downward_adjus_cpe = None,
                                             ussgl497200_downward_adjus_cpe = None,
                                             ussgl498100_upward_adjustm_cpe = randint(1,1000),
                                             ussgl498200_upward_adjustm_cpe = None)

    assert number_of_errors(_FILE, database, models=[award_fin_amount, award_fin_one, award_fin_two, award_fin_three]) == 0

def test_failure(database):
    """ Test with amount missing and no ussgl fields populated, also with some ussgl fields 0 """
    award_fin_empty = AwardFinancialFactory(transaction_obligated_amou = None,
                                             ussgl480100_undelivered_or_cpe = None,
                                             ussgl480100_undelivered_or_fyb = None,
		                                     ussgl480200_undelivered_or_cpe = None,
                                             ussgl480200_undelivered_or_fyb = None,
                                             ussgl483100_undelivered_or_cpe = None,
                                             ussgl483200_undelivered_or_cpe = None,
                                             ussgl487100_downward_adjus_cpe = None,
                                             ussgl487200_downward_adjus_cpe = None,
                                             ussgl488100_upward_adjustm_cpe = None,
                                             ussgl488200_upward_adjustm_cpe = None,
                                             ussgl490100_delivered_orde_cpe = None,
                                             ussgl490100_delivered_orde_fyb = None,
                                             ussgl490200_delivered_orde_cpe = None,
                                             ussgl490800_authority_outl_cpe = None,
                                             ussgl490800_authority_outl_fyb = None,
                                             ussgl493100_delivered_orde_cpe = None,
                                             ussgl497100_downward_adjus_cpe = None,
                                             ussgl497200_downward_adjus_cpe = None,
                                             ussgl498100_upward_adjustm_cpe = None,
                                             ussgl498200_upward_adjustm_cpe = None)
    award_fin_zeros = AwardFinancialFactory(transaction_obligated_amou = None,
                                             ussgl480100_undelivered_or_cpe = None,
                                             ussgl480100_undelivered_or_fyb = None,
		                                     ussgl480200_undelivered_or_cpe = 0,
                                             ussgl480200_undelivered_or_fyb = None,
                                             ussgl483100_undelivered_or_cpe = None,
                                             ussgl483200_undelivered_or_cpe = None,
                                             ussgl487100_downward_adjus_cpe = None,
                                             ussgl487200_downward_adjus_cpe = None,
                                             ussgl488100_upward_adjustm_cpe = None,
                                             ussgl488200_upward_adjustm_cpe = None,
                                             ussgl490100_delivered_orde_cpe = None,
                                             ussgl490100_delivered_orde_fyb = None,
                                             ussgl490200_delivered_orde_cpe = None,
                                             ussgl490800_authority_outl_cpe = None,
                                             ussgl490800_authority_outl_fyb = 0,
                                             ussgl493100_delivered_orde_cpe = None,
                                             ussgl497100_downward_adjus_cpe = None,
                                             ussgl497200_downward_adjus_cpe = None,
                                             ussgl498100_upward_adjustm_cpe = None,
                                             ussgl498200_upward_adjustm_cpe = 0)

    assert number_of_errors(_FILE, database, models=[award_fin_empty, award_fin_zeros]) == 2
