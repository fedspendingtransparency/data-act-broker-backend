from random import choice
from string import ascii_uppercase, ascii_lowercase, digits
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactcore.factories.staging import AwardFinancialFactory, AwardProcurementFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c23_award_financial_1'


def test_column_headers(database):
    expected_subset = {"row_number", "transaction_obligated_amou_sum", "federal_action_obligation_sum"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset <= actual


def test_success(database):
    """ Test that a four digit object class with no flag is a success, and a three digit object class with
        a flag is a success. Ignores rows with parent_award_id from AwardFinancialFactory and doesn't care about
        parent_award_id in AwardProcurementFactory """
    # Create cgac
    cgac = CGACFactory(cgac_code="123")

    # Create a 12 character random piid
    piid_1 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    piid_2 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    piid_3 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid rows
    af_1_row_1 = AwardFinancialFactory(transaction_obligated_amou=1100, piid=piid_1, parent_award_id='',
                                       allocation_transfer_agency=None)
    af_1_row_2 = AwardFinancialFactory(transaction_obligated_amou=11, piid=piid_1, parent_award_id=None,
                                       allocation_transfer_agency=None)
    # Ignored because it has a paid
    af_1_row_3 = AwardFinancialFactory(transaction_obligated_amou=11, piid=piid_1, parent_award_id="Test",
                                       allocation_transfer_agency=None)

    # Add a row for a different piid
    af_2_row_1 = AwardFinancialFactory(transaction_obligated_amou=9900, piid=piid_2, parent_award_id=None,
                                       allocation_transfer_agency=None)
    # Valid, matching ata/aid, not ignored
    af_2_row_2 = AwardFinancialFactory(transaction_obligated_amou=9, piid=piid_2, parent_award_id=None,
                                       allocation_transfer_agency="123", agency_identifier="123")
    # Invalid, not matching ata/aid, not ignored
    af_2_row_3 = AwardFinancialFactory(transaction_obligated_amou=90, piid=piid_2, parent_award_id=None,
                                       allocation_transfer_agency="345", agency_identifier="123")

    # Third piid with all rows ignored because one has a valid ATA different from AID
    af_3_row_1 = AwardFinancialFactory(transaction_obligated_amou=8888, piid=piid_3, parent_award_id=None,
                                       allocation_transfer_agency="123", agency_identifier="345")
    af_3_row_2 = AwardFinancialFactory(transaction_obligated_amou=8888, piid=piid_3, parent_award_id=None,
                                       allocation_transfer_agency=None)

    # Sum all of these should be equal to that of first piid
    ap_1_row_1 = AwardProcurementFactory(piid=piid_1, parent_award_id=None, federal_action_obligation=-1100)
    ap_1_row_2 = AwardProcurementFactory(piid=piid_1, parent_award_id=None, federal_action_obligation=-10)
    ap_1_row_3 = AwardProcurementFactory(piid=piid_1, parent_award_id=None, federal_action_obligation=-1)
    # Checking second piid
    ap_2 = AwardProcurementFactory(piid=piid_2, parent_award_id="1234", federal_action_obligation=-9999)
    # This one doesn't match but will be ignored
    ap_3 = AwardProcurementFactory(piid=piid_3, parent_award_id=None, federal_action_obligation=-9999)

    errors = number_of_errors(_FILE, database, models=[cgac, af_1_row_1, af_1_row_2, af_1_row_3, af_2_row_1, af_2_row_2,
                                                       af_2_row_3, af_3_row_1, af_3_row_2, ap_1_row_1, ap_1_row_2,
                                                       ap_1_row_3, ap_2, ap_3])
    assert errors == 0


def test_failure(database):
    """ Test that a three digit object class with no flag is an error """
    # Create cgac
    cgac = CGACFactory(cgac_code="123")

    # Create a 12 character random piid
    piid_1 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    piid_2 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    piid_3 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    piid_4 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # No ATA, not matching (off by 1)
    af_1_row_1 = AwardFinancialFactory(transaction_obligated_amou=1100, piid=piid_1, parent_award_id='',
                                       allocation_transfer_agency=None)
    af_1_row_2 = AwardFinancialFactory(transaction_obligated_amou=11, piid=piid_1, parent_award_id=None,
                                       allocation_transfer_agency=None)

    # No ATA, not matching, one record, no paid
    af_2 = AwardFinancialFactory(transaction_obligated_amou=9999, piid=piid_2, parent_award_id=None,
                                 allocation_transfer_agency=None)

    # ATA but valid, matching ATA, should not be ignored
    af_3 = AwardFinancialFactory(transaction_obligated_amou=11, piid=piid_3, parent_award_id=None,
                                 allocation_transfer_agency="123", agency_identifier="123")

    # ATA not matching but invalid, should not be ignored
    af_4 = AwardFinancialFactory(transaction_obligated_amou=11, piid=piid_4, parent_award_id=None,
                                 allocation_transfer_agency="345", agency_identifier="123")

    # Award Procurement portion of checks
    # Sum of all these would be sum of piid_1 af if one wasn't ignored
    ap_1_row_1 = AwardProcurementFactory(piid=piid_1, parent_award_id=None, federal_action_obligation=-1100)
    ap_1_row_2 = AwardProcurementFactory(piid=piid_1, parent_award_id=None, federal_action_obligation=-10)
    # second piid that simply doesn't match
    ap_2 = AwardProcurementFactory(piid=piid_2, parent_award_id=None, federal_action_obligation=-1111)
    # third piid that should not be ignored because ATA is present but matches
    ap_3 = AwardProcurementFactory(piid=piid_3, parent_award_id=None, federal_action_obligation=0)
    # fourth piid that should not be ignored because ATA is present and doesn't match but is invalid
    ap_4 = AwardProcurementFactory(piid=piid_4, parent_award_id=None, federal_action_obligation=0)

    errors = number_of_errors(_FILE, database, models=[af_1_row_1, af_1_row_2, af_2, af_3, af_4, ap_1_row_1, ap_1_row_2,
                                                       ap_2, ap_3, ap_4])
    assert errors == 4
