from random import choice
from string import ascii_uppercase, ascii_lowercase, digits
from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AwardFinancialFactory, AwardProcurementFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "c23_award_financial_1"


def test_column_headers(database):
    expected_subset = {
        "source_row_number",
        "source_value_piid",
        "source_value_transaction_obligated_amou_sum",
        "target_value_federal_action_obligation_sum",
        "difference",
        "uniqueid_PIID",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test For each unique PIID in File C, the sum of each TransactionObligatedAmount should match (but with opposite
    signs) the sum of the FederalActionObligation reported in D1. This rule does not apply if the ATA field is
    populated and is different from the Agency ID. Ignore rows that contain a PAID.
    """
    # Create a 12 character random piid
    piid_1 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    piid_2 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    piid_3 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    piid_4 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    sub_id = 1
    submission = SubmissionFactory(submission_id=sub_id, cgac_code='001')

    # First piid rows
    af_1_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100, piid=piid_1.lower(), parent_award_id="", allocation_transfer_agency=None, submission_id=sub_id
    )
    af_1_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid_1, parent_award_id=None, allocation_transfer_agency=None, submission_id=sub_id
    )
    # Ignored because it has a paid
    af_1_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid_1.upper(), parent_award_id="Test", allocation_transfer_agency=None, submission_id=sub_id
    )
    # Ignored row because it has non-matching ATA/AID but the rest of the sum works
    af_1_row_4 = AwardFinancialFactory(
        transaction_obligated_amou=99,
        piid=piid_1,
        parent_award_id=None,
        allocation_transfer_agency="good",
        agency_identifier="bad", submission_id=sub_id
    )

    # Add a row for a different piid
    af_2_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=9900, piid=piid_2.lower(), parent_award_id=None, allocation_transfer_agency=None, submission_id=sub_id
    )
    # Matching ata/aid, not ignored
    af_2_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=99,
        piid=piid_2,
        parent_award_id=None,
        allocation_transfer_agency="123",
        agency_identifier="123", submission_id=sub_id
    )
    # Not matching ata/aid, ignored
    af_2_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=10,
        piid=piid_2.upper(),
        parent_award_id=None,
        allocation_transfer_agency="345",
        agency_identifier="123", submission_id=sub_id
    )

    # Third ignored because it has an ATA different from AID
    af_3 = AwardFinancialFactory(
        transaction_obligated_amou=8888,
        piid=piid_3.lower(),
        parent_award_id=None,
        allocation_transfer_agency="123",
        agency_identifier="345", submission_id=sub_id
    )

    # No TOA in File C, ignored
    af_4 = AwardFinancialFactory(
        transaction_obligated_amou=None,
        piid=piid_4.lower(),
        parent_award_id=None,
        allocation_transfer_agency="123",
        agency_identifier="123", submission_id=sub_id
    )

    # Sum all of these should be equal to that of first piid
    ap_1_row_1 = AwardProcurementFactory(piid=piid_1, parent_award_id=None, federal_action_obligation=-1100, submission_id=sub_id)
    ap_1_row_2 = AwardProcurementFactory(piid=piid_1.lower(), parent_award_id=None, federal_action_obligation=-10, submission_id=sub_id)
    ap_1_row_3 = AwardProcurementFactory(piid=piid_1, parent_award_id=None, federal_action_obligation=-1, submission_id=sub_id)
    # Checking second piid
    ap_2 = AwardProcurementFactory(piid=piid_2, parent_award_id="1234", federal_action_obligation=-9999, submission_id=sub_id)
    # This one doesn't match but will be ignored
    ap_3 = AwardProcurementFactory(piid=piid_3, parent_award_id=None, federal_action_obligation=-9999, submission_id=sub_id)
    # This one matches but will be ignored
    ap_4 = AwardProcurementFactory(piid=piid_4, parent_award_id=None, federal_action_obligation=-9999, submission_id=sub_id)

    errors = number_of_errors(
        _FILE,
        database,
        models=[
            af_1_row_1,
            af_1_row_2,
            af_1_row_3,
            af_1_row_4,
            af_2_row_1,
            af_2_row_2,
            af_2_row_3,
            af_3,
            af_4,
            ap_1_row_1,
            ap_1_row_2,
            ap_1_row_3,
            ap_2,
            ap_3,
            ap_4,
        ],
        submission=submission
    )
    assert errors == 0


def test_success_submitting_ata(database):
    """Test for each unique PIID in File C, the sum of each TransactionObligatedAmount should match (but with opposite
    signs) the sum of the FederalActionObligation reported in D1. This rule does not apply if the ATA field is
    populated and is different from the Agency ID. Ignore rows that contain a PAID.
    Test CGAC/FREC match the ATA
    """

    cgac = CGACFactory(cgac_code="001")
    frec = FRECFactory(frec_code="0001", cgac=cgac)

    # Basic CGAC test
    sub_id = 1
    submission = SubmissionFactory(submission_id=sub_id, cgac_code='001', frec_code=None)
    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid rows
    af_1_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100, piid=piid.lower(), parent_award_id="", allocation_transfer_agency="001", agency_identifier="002",
        submission_id=sub_id
    )
    af_1_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid, parent_award_id=None, allocation_transfer_agency="001", agency_identifier="002",
        submission_id=sub_id
    )
    # Ignored because it has a different ATA
    af_1_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid.upper(), parent_award_id=None, allocation_transfer_agency="003", agency_identifier="002",
        submission_id=sub_id
    )

    # Sum all of these should be equal to that of first piid
    ap_1_row_1 = AwardProcurementFactory(piid=piid, parent_award_id=None, federal_action_obligation=-1100,
                                         submission_id=sub_id)
    ap_1_row_2 = AwardProcurementFactory(piid=piid.lower(), parent_award_id=None, federal_action_obligation=-10,
                                         submission_id=sub_id)
    ap_1_row_3 = AwardProcurementFactory(piid=piid, parent_award_id=None, federal_action_obligation=-1,
                                         submission_id=sub_id)

    errors = number_of_errors(
        _FILE,
        database,
        models=[
            cgac,
            frec,
            af_1_row_1,
            af_1_row_2,
            af_1_row_3,
            ap_1_row_1,
            ap_1_row_2,
            ap_1_row_3,
        ],
        submission=submission
    )
    assert errors == 0

    # FREC test, should align with the associated CGAC code
    sub_id = 2
    submission = SubmissionFactory(submission_id=sub_id, cgac_code=None, frec_code="0001")
    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid rows
    af_2_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100, piid=piid.lower(), parent_award_id="", allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id
    )
    af_2_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid, parent_award_id=None, allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id
    )
    # Ignored because it has a different ATA
    af_2_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid.upper(), parent_award_id=None, allocation_transfer_agency="003",
        agency_identifier="002",
        submission_id=sub_id
    )

    # Sum all of these should be equal to that of first piid
    ap_2_row_1 = AwardProcurementFactory(piid=piid, parent_award_id=None, federal_action_obligation=-1100,
                                         submission_id=sub_id)
    ap_2_row_2 = AwardProcurementFactory(piid=piid.lower(), parent_award_id=None, federal_action_obligation=-10,
                                         submission_id=sub_id)
    ap_2_row_3 = AwardProcurementFactory(piid=piid, parent_award_id=None, federal_action_obligation=-1,
                                         submission_id=sub_id)

    errors = number_of_errors(
        _FILE,
        database,
        models=[
            cgac,
            frec,
            af_2_row_1,
            af_2_row_2,
            af_2_row_3,
            ap_2_row_1,
            ap_2_row_2,
            ap_2_row_3,
        ],
        submission=submission
    )
    assert errors == 0

    # Hardcoded CGAC test
    sub_id = 3
    submission = SubmissionFactory(submission_id=sub_id, cgac_code="097", frec_code=None)
    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid rows
    af_3_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100, piid=piid.lower(), parent_award_id="", allocation_transfer_agency="017",
        agency_identifier="002",
        submission_id=sub_id
    )
    af_3_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid, parent_award_id=None, allocation_transfer_agency="057",
        agency_identifier="002",
        submission_id=sub_id
    )
    # Ignored because it has a different ATA
    af_3_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid.upper(), parent_award_id=None, allocation_transfer_agency="003",
        agency_identifier="002",
        submission_id=sub_id
    )

    # Sum all of these should be equal to that of first piid
    ap_3_row_1 = AwardProcurementFactory(piid=piid, parent_award_id=None, federal_action_obligation=-1100,
                                         submission_id=sub_id)
    ap_3_row_2 = AwardProcurementFactory(piid=piid.lower(), parent_award_id=None, federal_action_obligation=-10,
                                         submission_id=sub_id)
    ap_3_row_3 = AwardProcurementFactory(piid=piid, parent_award_id=None, federal_action_obligation=-1,
                                         submission_id=sub_id)

    errors = number_of_errors(
        _FILE,
        database,
        models=[
            cgac,
            frec,
            af_3_row_1,
            af_3_row_2,
            af_3_row_3,
            ap_3_row_1,
            ap_3_row_2,
            ap_3_row_3,
        ],
        submission=submission
    )
    assert errors == 0

    # Hardcoded FREC test
    cgac = CGACFactory(cgac_code="005")
    frec = FRECFactory(frec_code="1100", cgac=cgac)
    sub_id = 4
    submission = SubmissionFactory(submission_id=sub_id, cgac_code=None, frec_code="1100")
    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid rows
    af_4_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100, piid=piid.lower(), parent_award_id="", allocation_transfer_agency="256",
        agency_identifier="002",
        submission_id=sub_id
    )
    af_4_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid, parent_award_id=None, allocation_transfer_agency="256",
        agency_identifier="002",
        submission_id=sub_id
    )
    # Ignored because it has a different ATA even though that ATA matches the FREC table
    af_4_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid.upper(), parent_award_id=None, allocation_transfer_agency="005",
        agency_identifier="002",
        submission_id=sub_id
    )

    # Sum all of these should be equal to that of first piid
    ap_4_row_1 = AwardProcurementFactory(piid=piid, parent_award_id=None, federal_action_obligation=-1100,
                                         submission_id=sub_id)
    ap_4_row_2 = AwardProcurementFactory(piid=piid.lower(), parent_award_id=None, federal_action_obligation=-10,
                                         submission_id=sub_id)
    ap_4_row_3 = AwardProcurementFactory(piid=piid, parent_award_id=None, federal_action_obligation=-1,
                                         submission_id=sub_id)

    errors = number_of_errors(
        _FILE,
        database,
        models=[
            cgac,
            frec,
            af_4_row_1,
            af_4_row_2,
            af_4_row_3,
            ap_4_row_1,
            ap_4_row_2,
            ap_4_row_3,
        ],
        submission=submission
    )
    assert errors == 0


def test_failure(database):
    """Test failure for each unique PIID in File C, the sum of each TransactionObligatedAmount should match (but with
    opposite signs) the sum of the FederalActionObligation reported in D1. This rule does not apply if the ATA field
    is populated and is different from the Agency ID. Ignore rows that contain a PAID.
    """
    # Create a 12 character random piid
    piid_1 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    piid_2 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    piid_3 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    piid_4 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    sub_id = 1
    submission = SubmissionFactory(submission_id=sub_id, cgac_code='001')

    # No ATA, not matching (off by 1)
    af_1_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100, piid=piid_1, parent_award_id="", allocation_transfer_agency=None, submission_id=sub_id
    )
    af_1_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=9, piid=piid_1, parent_award_id=None, allocation_transfer_agency=None, submission_id=sub_id
    )
    # Ignore row that would make it add up right if it was counted because of non-matching ATA/AID
    af_1_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=1,
        piid=piid_1.lower(),
        parent_award_id="",
        allocation_transfer_agency="good",
        agency_identifier="bad", submission_id=sub_id
    )

    # No ATA, not matching, one record, no paid
    af_2 = AwardFinancialFactory(
        transaction_obligated_amou=9999, piid=piid_2, parent_award_id=None, allocation_transfer_agency=None, submission_id=sub_id
    )

    # Matching ATA, should not be ignored
    af_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid_3,
        parent_award_id=None,
        allocation_transfer_agency="123",
        agency_identifier="123", submission_id=sub_id
    )

    # Not ignored with TOA of 0
    af_4 = AwardFinancialFactory(
        transaction_obligated_amou=0,
        piid=piid_4.lower(),
        parent_award_id=None,
        allocation_transfer_agency="123",
        agency_identifier="123", submission_id=sub_id
    )

    # Award Procurement portion of checks
    # Sum of all these would be sum of piid_1 af if one wasn't ignored
    ap_1_row_1 = AwardProcurementFactory(piid=piid_1, parent_award_id=None, federal_action_obligation=-1100, submission_id=sub_id)
    ap_1_row_2 = AwardProcurementFactory(piid=piid_1, parent_award_id=None, federal_action_obligation=-10, submission_id=sub_id)
    # second piid that simply doesn't match
    ap_2 = AwardProcurementFactory(piid=piid_2, parent_award_id=None, federal_action_obligation=-1111, submission_id=sub_id)
    # third piid that should not be ignored because ATA is present but matches
    ap_3 = AwardProcurementFactory(piid=piid_3, parent_award_id=None, federal_action_obligation=0, submission_id=sub_id)
    # fourth piid that should not be ignored because TOA is present
    ap_4 = AwardProcurementFactory(piid=piid_4, parent_award_id=None, federal_action_obligation=1, submission_id=sub_id)

    errors = number_of_errors(
        _FILE,
        database,
        models=[af_1_row_1, af_1_row_2, af_1_row_3, af_2, af_3, af_4, ap_1_row_1, ap_1_row_2, ap_2, ap_3, ap_4],
        submission=submission
    )
    assert errors == 4


def test_failure_submitting_ata(database):
    """Test failure for each unique PIID in File C, the sum of each TransactionObligatedAmount should match (but with
    opposite signs) the sum of the FederalActionObligation reported in D1. This rule does not apply if the ATA field is
    populated and is different from the Agency ID. Ignore rows that contain a PAID.
    Test CGAC/FREC match the ATA
    """

    cgac = CGACFactory(cgac_code="001")
    frec = FRECFactory(frec_code="0001", cgac=cgac)

    # Basic CGAC test
    sub_id = 1
    submission = SubmissionFactory(submission_id=sub_id, cgac_code='001', frec_code=None)
    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid rows
    af_1_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100, piid=piid.lower(), parent_award_id="", allocation_transfer_agency="001", agency_identifier="002",
        submission_id=sub_id
    )
    af_1_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid, parent_award_id=None, allocation_transfer_agency="001", agency_identifier="002",
        submission_id=sub_id
    )
    # Ignored because it has a different ATA
    af_1_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid.upper(), parent_award_id=None, allocation_transfer_agency="003", agency_identifier="002",
        submission_id=sub_id
    )

    # These add up to the sum of all 3, including the one that was ignored
    ap_1_row_1 = AwardProcurementFactory(piid=piid, parent_award_id=None, federal_action_obligation=-1100,
                                         submission_id=sub_id)
    ap_1_row_2 = AwardProcurementFactory(piid=piid.lower(), parent_award_id=None, federal_action_obligation=-10,
                                         submission_id=sub_id)
    ap_1_row_3 = AwardProcurementFactory(piid=piid, parent_award_id=None, federal_action_obligation=-12,
                                         submission_id=sub_id)

    errors = number_of_errors(
        _FILE,
        database,
        models=[
            af_1_row_1,
            af_1_row_2,
            af_1_row_3,
            ap_1_row_1,
            ap_1_row_2,
            ap_1_row_3,
        ],
        submission=submission
    )
    assert errors == 1

    # FREC test, should align with the associated CGAC code
    sub_id = 2
    submission = SubmissionFactory(submission_id=sub_id, cgac_code=None, frec_code="0001")
    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid rows
    af_2_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100, piid=piid.lower(), parent_award_id="", allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id
    )
    af_2_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid, parent_award_id=None, allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id
    )
    # Ignored because it has a different ATA
    af_2_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid.upper(), parent_award_id=None, allocation_transfer_agency="003",
        agency_identifier="002",
        submission_id=sub_id
    )

    # Sum all of these should be equal to that of first piid
    ap_2_row_1 = AwardProcurementFactory(piid=piid, parent_award_id=None, federal_action_obligation=-1100,
                                         submission_id=sub_id)
    ap_2_row_2 = AwardProcurementFactory(piid=piid.lower(), parent_award_id=None, federal_action_obligation=-10,
                                         submission_id=sub_id)
    ap_2_row_3 = AwardProcurementFactory(piid=piid, parent_award_id=None, federal_action_obligation=-12,
                                         submission_id=sub_id)

    errors = number_of_errors(
        _FILE,
        database,
        models=[
            cgac,
            frec,
            af_2_row_1,
            af_2_row_2,
            af_2_row_3,
            ap_2_row_1,
            ap_2_row_2,
            ap_2_row_3,
        ],
        submission=submission
    )
    assert errors == 1

    # Hardcoded CGAC test
    sub_id = 3
    submission = SubmissionFactory(submission_id=sub_id, cgac_code="097", frec_code=None)
    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid rows
    af_3_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100, piid=piid.lower(), parent_award_id="", allocation_transfer_agency="017",
        agency_identifier="002",
        submission_id=sub_id
    )
    af_3_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid, parent_award_id=None, allocation_transfer_agency="057",
        agency_identifier="002",
        submission_id=sub_id
    )
    # Ignored because it has a different ATA
    af_3_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid.upper(), parent_award_id=None, allocation_transfer_agency="003",
        agency_identifier="002",
        submission_id=sub_id
    )

    # Sum all of these should be equal to that of first piid
    ap_3_row_1 = AwardProcurementFactory(piid=piid, parent_award_id=None, federal_action_obligation=-1100,
                                         submission_id=sub_id)
    ap_3_row_2 = AwardProcurementFactory(piid=piid.lower(), parent_award_id=None, federal_action_obligation=-10,
                                         submission_id=sub_id)
    ap_3_row_3 = AwardProcurementFactory(piid=piid, parent_award_id=None, federal_action_obligation=-12,
                                         submission_id=sub_id)

    errors = number_of_errors(
        _FILE,
        database,
        models=[
            cgac,
            frec,
            af_3_row_1,
            af_3_row_2,
            af_3_row_3,
            ap_3_row_1,
            ap_3_row_2,
            ap_3_row_3,
        ],
        submission=submission
    )
    assert errors == 1

    # Hardcoded FREC test
    cgac = CGACFactory(cgac_code="005")
    frec = FRECFactory(frec_code="1100", cgac=cgac)
    sub_id = 4
    submission = SubmissionFactory(submission_id=sub_id, cgac_code=None, frec_code="1100")
    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid rows
    af_4_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100, piid=piid.lower(), parent_award_id="", allocation_transfer_agency="256",
        agency_identifier="002",
        submission_id=sub_id
    )
    af_4_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid, parent_award_id=None, allocation_transfer_agency="256",
        agency_identifier="002",
        submission_id=sub_id
    )
    # Ignored because it has a different ATA even though that ATA matches the FREC table
    af_4_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11, piid=piid.upper(), parent_award_id=None, allocation_transfer_agency="005",
        agency_identifier="002",
        submission_id=sub_id
    )

    # Sum all of these should be equal to that of first piid
    ap_4_row_1 = AwardProcurementFactory(piid=piid, parent_award_id=None, federal_action_obligation=-1100,
                                         submission_id=sub_id)
    ap_4_row_2 = AwardProcurementFactory(piid=piid.lower(), parent_award_id=None, federal_action_obligation=-10,
                                         submission_id=sub_id)
    ap_4_row_3 = AwardProcurementFactory(piid=piid, parent_award_id=None, federal_action_obligation=-12,
                                         submission_id=sub_id)

    errors = number_of_errors(
        _FILE,
        database,
        models=[
            cgac,
            frec,
            af_4_row_1,
            af_4_row_2,
            af_4_row_3,
            ap_4_row_1,
            ap_4_row_2,
            ap_4_row_3,
        ],
        submission=submission
    )
    assert errors == 1
