from random import choice
from string import ascii_uppercase, ascii_lowercase, digits
from tests.unit.dataactcore.factories.staging import AwardFinancialFactory, AwardProcurementFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c23_award_financial_2'


def test_column_headers(database):
    expected_subset = {"row_number", "transaction_obligated_amou_sum", "federal_action_obligation_sum"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset <= actual


def test_success(database):
    """ Test for each unique combination of PIID/ParentAwardId in File C, the sum of each TransactionObligatedAmount
        should match (but with opposite signs) the sum of the FederalActionObligation reported in D1. This rule does not
        apply if the ATA field is populated and is different from the Agency ID.
    """
    # Create a 12 character random parent_award_id
    paid_1 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    paid_2 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    paid_3 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    piid = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    af_1_row_1 = AwardFinancialFactory(transaction_obligated_amou=1100, piid=piid.lower(),
                                       parent_award_id=paid_1.lower(), allocation_transfer_agency=None)
    af_1_row_2 = AwardFinancialFactory(transaction_obligated_amou=11, piid=piid, parent_award_id=paid_1,
                                       allocation_transfer_agency=None)
    # next 2 rows ignored because they don't have a PIID
    af_1_row_3 = AwardFinancialFactory(transaction_obligated_amou=11, piid=None, parent_award_id=paid_1,
                                       allocation_transfer_agency=None)
    af_1_row_4 = AwardFinancialFactory(transaction_obligated_amou=11, piid='', parent_award_id=paid_1.upper(),
                                       allocation_transfer_agency=None)

    # Two entries that aren't ignored because they have matching ATA/AID or no ATA
    af_2_row_1 = AwardFinancialFactory(transaction_obligated_amou=9900, piid=piid, parent_award_id=paid_2,
                                       allocation_transfer_agency=None)
    af_2_row_2 = AwardFinancialFactory(transaction_obligated_amou=99, piid=piid.lower(), parent_award_id=paid_2.lower(),
                                       allocation_transfer_agency="good", agency_identifier="good")

    # Entry that is ignored because the ATA/AID don't match
    af_3 = AwardFinancialFactory(transaction_obligated_amou=8888, piid=piid, parent_award_id=paid_3,
                                 allocation_transfer_agency="good", agency_identifier="bad")

    # Combine these to match paid_1
    ap_1_row_1 = AwardProcurementFactory(parent_award_id=paid_1.lower(), piid=piid.lower(),
                                         federal_action_obligation=-1100)
    ap_1_row_2 = AwardProcurementFactory(parent_award_id=paid_1.upper(), piid=piid, federal_action_obligation=-10)
    ap_1_row_3 = AwardProcurementFactory(parent_award_id=paid_1, piid=piid.upper(), federal_action_obligation=-1)
    # This one should match because nothing is ignored
    ap_2 = AwardProcurementFactory(parent_award_id=paid_2, piid=piid, federal_action_obligation=-9999)
    # This is ignored because the ATA/AID for this one don't match
    ap_3 = AwardProcurementFactory(parent_award_id=paid_3, piid=piid, federal_action_obligation=-9999)

    errors = number_of_errors(_FILE, database, models=[af_1_row_1, af_1_row_2, af_1_row_3, af_1_row_4, af_2_row_1,
                                                       af_2_row_2, af_3, ap_1_row_1, ap_1_row_2, ap_1_row_3, ap_2,
                                                       ap_3])
    assert errors == 0


def test_failure(database):
    """ Test failure for each unique combination of PIID/ParentAwardId in File C, the sum of each
        TransactionObligatedAmount should match (but with opposite signs) the sum of the FederalActionObligation
        reported in D1. This rule does not apply if the ATA field is populated and is different from the Agency ID.
    """
    # Create a 12 character random parent_award_id
    paid_1 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    paid_2 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    piid = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # Basic sum, row 3 is ignored in this sum because it doesn't have a paid
    af_1_row_1 = AwardFinancialFactory(transaction_obligated_amou=1100, piid=piid.lower(), parent_award_id=paid_1,
                                       allocation_transfer_agency=None)
    af_1_row_2 = AwardFinancialFactory(transaction_obligated_amou=11, piid=piid, parent_award_id=paid_1.lower(),
                                       allocation_transfer_agency=None)
    af_1_row_3 = AwardFinancialFactory(transaction_obligated_amou=11, piid=piid.upper(), parent_award_id=None,
                                       allocation_transfer_agency=None)
    # Same ATA/AID or no ATA sum
    af_2_row_1 = AwardFinancialFactory(transaction_obligated_amou=1111, piid=piid, parent_award_id=paid_2.lower(),
                                       allocation_transfer_agency=None)
    af_2_row_2 = AwardFinancialFactory(transaction_obligated_amou=1111, piid=piid.lower(), parent_award_id=paid_2,
                                       allocation_transfer_agency="good", agency_identifier="good")

    # Sum of these values doesn't add up (ignoring third one because it has a different paid)
    ap_1_row_1 = AwardProcurementFactory(parent_award_id=paid_1, piid=piid.lower(), federal_action_obligation=-1100)
    ap_1_row_2 = AwardProcurementFactory(parent_award_id=paid_1.lower(), piid=piid, federal_action_obligation=-10)
    ap_1_row_3 = AwardProcurementFactory(parent_award_id="1234", piid=piid.upper(), federal_action_obligation=-1)
    # Sum of the two above should be both of them, not just one
    ap_2 = AwardProcurementFactory(parent_award_id=paid_2, piid=piid, federal_action_obligation=-1111)

    errors = number_of_errors(_FILE, database, models=[af_1_row_1, af_1_row_2, af_1_row_3, af_2_row_1, af_2_row_2,
                                                       ap_1_row_1, ap_1_row_2, ap_1_row_3, ap_2])
    assert errors == 2
