from random import choice
from string import ascii_uppercase, ascii_lowercase, digits
from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AwardFinancialFactory, AwardProcurementFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "c23_award_financial_2"


def test_column_headers(database):
    expected_subset = {
        "source_row_number",
        "source_value_piid",
        "source_value_parent_award_id",
        "source_value_transaction_obligated_amou_sum",
        "target_value_federal_action_obligation_sum",
        "difference",
        "uniqueid_PIID",
        "uniqueid_ParentAwardId",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test for each unique combination of PIID/ParentAwardId in File C, the sum of each TransactionObligatedAmount
    should match (but with opposite signs) the sum of the FederalActionObligation reported in D1. This rule applies when
    the ATA and AID are the same, when the ATA is the submitting agency, or when the ATA is blank.
    """
    # Create a 12 character random parent_award_id
    paid_1 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    paid_2 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    paid_3 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    paid_4 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    sub_id = 1
    submission = SubmissionFactory(submission_id=sub_id, cgac_code="001")

    af_1_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        piid=piid.lower(),
        parent_award_id=paid_1.lower(),
        allocation_transfer_agency=None,
        submission_id=sub_id,
    )
    af_1_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid,
        parent_award_id=paid_1,
        allocation_transfer_agency=None,
        submission_id=sub_id,
    )
    # next 2 rows ignored because they don't have a PIID
    af_1_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=None,
        parent_award_id=paid_1,
        allocation_transfer_agency=None,
        submission_id=sub_id,
    )
    af_1_row_4 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid="",
        parent_award_id=paid_1.upper(),
        allocation_transfer_agency=None,
        submission_id=sub_id,
    )
    # row ignored because it has an ata that doesn't match the aid, but the rest of the entry should match
    af_1_row_5 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid,
        parent_award_id=paid_1.upper(),
        allocation_transfer_agency="good",
        agency_identifier="bad",
        submission_id=sub_id,
    )

    # Two entries that aren't ignored because they have matching ATA/AID or no ATA
    af_2_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=9900,
        piid=piid,
        parent_award_id=paid_2,
        allocation_transfer_agency=None,
        submission_id=sub_id,
    )
    af_2_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=99,
        piid=piid.lower(),
        parent_award_id=paid_2.lower(),
        allocation_transfer_agency="good",
        agency_identifier="good",
        submission_id=sub_id,
    )

    # Entry that is ignored because the ATA/AID don't match
    af_3 = AwardFinancialFactory(
        transaction_obligated_amou=8888,
        piid=piid,
        parent_award_id=paid_3,
        allocation_transfer_agency="good",
        agency_identifier="bad",
        submission_id=sub_id,
    )

    # No TOA in File C, ignored
    af_4 = AwardFinancialFactory(
        transaction_obligated_amou=None,
        piid=piid.lower(),
        parent_award_id=paid_4,
        allocation_transfer_agency="good",
        agency_identifier="good",
        submission_id=sub_id,
    )

    # Combine these to match paid_1
    ap_1_row_1 = AwardProcurementFactory(
        parent_award_id=paid_1.lower(), piid=piid.lower(), federal_action_obligation=-1100, submission_id=sub_id
    )
    ap_1_row_2 = AwardProcurementFactory(
        parent_award_id=paid_1.upper(), piid=piid, federal_action_obligation=-10, submission_id=sub_id
    )
    ap_1_row_3 = AwardProcurementFactory(
        parent_award_id=paid_1, piid=piid.upper(), federal_action_obligation=-1, submission_id=sub_id
    )
    # This one should match because nothing is ignored
    ap_2 = AwardProcurementFactory(
        parent_award_id=paid_2, piid=piid, federal_action_obligation=-9999, submission_id=sub_id
    )
    # This is ignored because the ATA/AID for this one don't match
    ap_3 = AwardProcurementFactory(
        parent_award_id=paid_3, piid=piid, federal_action_obligation=-9999, submission_id=sub_id
    )
    # This one matches but will be ignored
    ap_4 = AwardProcurementFactory(
        parent_award_id=paid_4, piid=piid, federal_action_obligation=-9999, submission_id=sub_id
    )

    errors = number_of_errors(
        _FILE,
        database,
        models=[
            af_1_row_1,
            af_1_row_2,
            af_1_row_3,
            af_1_row_4,
            af_1_row_5,
            af_2_row_1,
            af_2_row_2,
            af_3,
            af_4,
            ap_1_row_1,
            ap_1_row_2,
            ap_1_row_3,
            ap_2,
            ap_3,
            ap_4,
        ],
        submission=submission,
    )
    assert errors == 0


def test_success_submitting_ata(database):
    """Test for each unique combination of PIID/ParentAwardId in File C, the sum of each TransactionObligatedAmount
    should match (but with opposite signs) the sum of the FederalActionObligation reported in D1. This rule applies when
    the ATA and AID are the same, when the ATA is the submitting agency, or when the ATA is blank.
    Test CGAC/FREC match the ATA
    """

    cgac = CGACFactory(cgac_code="001")
    frec = FRECFactory(frec_code="0001", cgac=cgac)

    # Basic CGAC test
    sub_id = 1
    submission = SubmissionFactory(submission_id=sub_id, cgac_code="001", frec_code=None)
    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    paid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid/paid rows
    af_1_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        piid=piid.lower(),
        parent_award_id=paid.upper(),
        allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id,
    )
    af_1_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid,
        parent_award_id=paid.lower(),
        allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id,
    )
    # Ignored because it has a different ATA
    af_1_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid.upper(),
        parent_award_id=paid,
        allocation_transfer_agency="003",
        agency_identifier="002",
        submission_id=sub_id,
    )

    # Sum all of these should be equal to that of first piid/paid
    ap_1_row_1 = AwardProcurementFactory(
        piid=piid, parent_award_id=paid.lower(), federal_action_obligation=-1100, submission_id=sub_id
    )
    ap_1_row_2 = AwardProcurementFactory(
        piid=piid.lower(), parent_award_id=paid.upper(), federal_action_obligation=-10, submission_id=sub_id
    )
    ap_1_row_3 = AwardProcurementFactory(
        piid=piid, parent_award_id=paid, federal_action_obligation=-1, submission_id=sub_id
    )

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
        submission=submission,
    )
    assert errors == 0

    # FREC test, should align with the associated CGAC code
    sub_id = 2
    submission = SubmissionFactory(submission_id=sub_id, cgac_code=None, frec_code="0001")
    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    paid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid/paid rows
    af_2_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        piid=piid.lower(),
        parent_award_id=paid,
        allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id,
    )
    af_2_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid,
        parent_award_id=paid.lower(),
        allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id,
    )
    # Ignored because it has a different ATA
    af_2_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid.upper(),
        parent_award_id=paid.upper(),
        allocation_transfer_agency="003",
        agency_identifier="002",
        submission_id=sub_id,
    )

    # Sum all of these should be equal to that of first piid/paid
    ap_2_row_1 = AwardProcurementFactory(
        piid=piid, parent_award_id=paid.lower(), federal_action_obligation=-1100, submission_id=sub_id
    )
    ap_2_row_2 = AwardProcurementFactory(
        piid=piid.lower(), parent_award_id=paid.upper(), federal_action_obligation=-10, submission_id=sub_id
    )
    ap_2_row_3 = AwardProcurementFactory(
        piid=piid, parent_award_id=paid, federal_action_obligation=-1, submission_id=sub_id
    )

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
        submission=submission,
    )
    assert errors == 0

    # Hardcoded CGAC test
    sub_id = 3
    submission = SubmissionFactory(submission_id=sub_id, cgac_code="097", frec_code=None)
    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    paid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid/paid rows
    af_3_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        piid=piid.lower(),
        parent_award_id=paid,
        allocation_transfer_agency="017",
        agency_identifier="002",
        submission_id=sub_id,
    )
    af_3_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid,
        parent_award_id=paid.lower(),
        allocation_transfer_agency="057",
        agency_identifier="002",
        submission_id=sub_id,
    )
    # Ignored because it has a different ATA
    af_3_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid.upper(),
        parent_award_id=paid.upper(),
        allocation_transfer_agency="003",
        agency_identifier="002",
        submission_id=sub_id,
    )

    # Sum all of these should be equal to that of first piid/paid
    ap_3_row_1 = AwardProcurementFactory(
        piid=piid, parent_award_id=paid, federal_action_obligation=-1100, submission_id=sub_id
    )
    ap_3_row_2 = AwardProcurementFactory(
        piid=piid.lower(), parent_award_id=paid.lower(), federal_action_obligation=-10, submission_id=sub_id
    )
    ap_3_row_3 = AwardProcurementFactory(
        piid=piid, parent_award_id=paid.upper(), federal_action_obligation=-1, submission_id=sub_id
    )

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
        submission=submission,
    )
    assert errors == 0

    # Hardcoded FREC test
    cgac = CGACFactory(cgac_code="005")
    frec = FRECFactory(frec_code="1100", cgac=cgac)
    sub_id = 4
    submission = SubmissionFactory(submission_id=sub_id, cgac_code=None, frec_code="1100")
    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    paid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid/paid rows
    af_4_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        piid=piid.lower(),
        parent_award_id=paid,
        allocation_transfer_agency="256",
        agency_identifier="002",
        submission_id=sub_id,
    )
    af_4_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid,
        parent_award_id=paid.lower(),
        allocation_transfer_agency="256",
        agency_identifier="002",
        submission_id=sub_id,
    )
    # Ignored because it has a different ATA even though that ATA matches the FREC table
    af_4_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid.upper(),
        parent_award_id=paid.upper(),
        allocation_transfer_agency="005",
        agency_identifier="002",
        submission_id=sub_id,
    )

    # Sum all of these should be equal to that of first piid/paid
    ap_4_row_1 = AwardProcurementFactory(
        piid=piid, parent_award_id=paid.lower(), federal_action_obligation=-1100, submission_id=sub_id
    )
    ap_4_row_2 = AwardProcurementFactory(
        piid=piid.lower(), parent_award_id=paid.upper(), federal_action_obligation=-10, submission_id=sub_id
    )
    ap_4_row_3 = AwardProcurementFactory(
        piid=piid, parent_award_id=paid, federal_action_obligation=-1, submission_id=sub_id
    )

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
        submission=submission,
    )
    assert errors == 0


def test_failure(database):
    """Test failure for each unique combination of PIID/ParentAwardId in File C, the sum of each
    TransactionObligatedAmount should match (but with opposite signs) the sum of the FederalActionObligation reported in
    D1. This rule applies when the ATA and AID are the same, when the ATA is the submitting agency, or when the ATA is
    blank.
    """
    # Create a 12 character random parent_award_id
    paid_1 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    paid_2 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    paid_3 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    sub_id = 1
    submission = SubmissionFactory(submission_id=sub_id, cgac_code="001")

    # Basic sum, row 3 is ignored in this sum because it doesn't have a paid and 4 is ignored because the ATA/AID don't
    # match
    af_1_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        piid=piid.lower(),
        parent_award_id=paid_1,
        allocation_transfer_agency=None,
        submission_id=sub_id,
    )
    af_1_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=9,
        piid=piid,
        parent_award_id=paid_1.lower(),
        allocation_transfer_agency=None,
        submission_id=sub_id,
    )
    af_1_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=1,
        piid=piid.upper(),
        parent_award_id=None,
        allocation_transfer_agency=None,
        submission_id=sub_id,
    )
    af_1_row_4 = AwardFinancialFactory(
        transaction_obligated_amou=1,
        piid=piid,
        parent_award_id=paid_1.upper(),
        allocation_transfer_agency="good",
        agency_identifier="bad",
        submission_id=sub_id,
    )
    # Same ATA/AID or no ATA sum
    af_2_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1111,
        piid=piid,
        parent_award_id=paid_2.lower(),
        allocation_transfer_agency=None,
        submission_id=sub_id,
    )
    af_2_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=1111,
        piid=piid.lower(),
        parent_award_id=paid_2,
        allocation_transfer_agency="good",
        agency_identifier="good",
        submission_id=sub_id,
    )
    # Not ignored with TOA of 0
    af_3 = AwardFinancialFactory(
        transaction_obligated_amou=0,
        piid=piid.lower(),
        parent_award_id=paid_3.lower(),
        allocation_transfer_agency="good",
        agency_identifier="good",
        submission_id=sub_id,
    )

    # Sum of these values doesn't add up (ignoring third one because it has a different paid)
    ap_1_row_1 = AwardProcurementFactory(
        parent_award_id=paid_1, piid=piid.lower(), federal_action_obligation=-1100, submission_id=sub_id
    )
    ap_1_row_2 = AwardProcurementFactory(
        parent_award_id=paid_1.lower(), piid=piid, federal_action_obligation=-10, submission_id=sub_id
    )
    ap_1_row_3 = AwardProcurementFactory(
        parent_award_id="1234", piid=piid.upper(), federal_action_obligation=-1, submission_id=sub_id
    )
    # Sum of the two above should be both of them, not just one
    ap_2 = AwardProcurementFactory(
        parent_award_id=paid_2, piid=piid, federal_action_obligation=-1111, submission_id=sub_id
    )
    # Should not be ignored because TOA is present
    ap_3 = AwardProcurementFactory(parent_award_id=paid_3, piid=piid, federal_action_obligation=1, submission_id=sub_id)

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
            af_3,
            ap_1_row_1,
            ap_1_row_2,
            ap_1_row_3,
            ap_2,
            ap_3,
        ],
        submission=submission,
    )
    assert errors == 3


def test_failure_submitting_ata(database):
    """Test failure for each unique combination of PIID/ParentAwardId in File C, the sum of each
    TransactionObligatedAmount should match (but with opposite signs) the sum of the FederalActionObligation reported in
    D1. This rule applies when the ATA and AID are the same, when the ATA is the submitting agency, or when the ATA is
    blank.
    Test CGAC/FREC match the ATA
    """

    cgac = CGACFactory(cgac_code="001")
    frec = FRECFactory(frec_code="0001", cgac=cgac)

    # Basic CGAC test
    sub_id = 1
    submission = SubmissionFactory(submission_id=sub_id, cgac_code="001", frec_code=None)
    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    paid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid/paid rows
    af_1_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        piid=piid.lower(),
        parent_award_id=paid.upper(),
        allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id,
    )
    af_1_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid,
        parent_award_id=paid.lower(),
        allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id,
    )
    # Ignored because it has a different ATA
    af_1_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid.upper(),
        parent_award_id=paid,
        allocation_transfer_agency="003",
        agency_identifier="002",
        submission_id=sub_id,
    )

    # These add up to the sum of all 3, including the one that was ignored
    ap_1_row_1 = AwardProcurementFactory(
        piid=piid, parent_award_id=paid, federal_action_obligation=-1100, submission_id=sub_id
    )
    ap_1_row_2 = AwardProcurementFactory(
        piid=piid.lower(), parent_award_id=paid.upper(), federal_action_obligation=-10, submission_id=sub_id
    )
    ap_1_row_3 = AwardProcurementFactory(
        piid=piid, parent_award_id=paid.lower(), federal_action_obligation=-12, submission_id=sub_id
    )

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
        submission=submission,
    )
    assert errors == 1

    # FREC test, should align with the associated CGAC code
    sub_id = 2
    submission = SubmissionFactory(submission_id=sub_id, cgac_code=None, frec_code="0001")
    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    paid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid/paid rows
    af_2_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        piid=piid.lower(),
        parent_award_id=paid.lower(),
        allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id,
    )
    af_2_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid,
        parent_award_id=paid,
        allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id,
    )
    # Ignored because it has a different ATA
    af_2_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid.upper(),
        parent_award_id=paid.upper(),
        allocation_transfer_agency="003",
        agency_identifier="002",
        submission_id=sub_id,
    )

    # Sum all of these should be equal to that of first piid/paid
    ap_2_row_1 = AwardProcurementFactory(
        piid=piid, parent_award_id=paid, federal_action_obligation=-1100, submission_id=sub_id
    )
    ap_2_row_2 = AwardProcurementFactory(
        piid=piid.lower(), parent_award_id=paid.upper(), federal_action_obligation=-10, submission_id=sub_id
    )
    ap_2_row_3 = AwardProcurementFactory(
        piid=piid, parent_award_id=paid.lower(), federal_action_obligation=-12, submission_id=sub_id
    )

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
        submission=submission,
    )
    assert errors == 1

    # Hardcoded CGAC test
    sub_id = 3
    submission = SubmissionFactory(submission_id=sub_id, cgac_code="097", frec_code=None)
    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    paid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid/paid rows
    af_3_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        piid=piid.lower(),
        parent_award_id=paid.lower(),
        allocation_transfer_agency="017",
        agency_identifier="002",
        submission_id=sub_id,
    )
    af_3_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid,
        parent_award_id=paid.upper(),
        allocation_transfer_agency="057",
        agency_identifier="002",
        submission_id=sub_id,
    )
    # Ignored because it has a different ATA
    af_3_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid.upper(),
        parent_award_id=paid,
        allocation_transfer_agency="003",
        agency_identifier="002",
        submission_id=sub_id,
    )

    # Sum all of these should be equal to that of first piid/paid
    ap_3_row_1 = AwardProcurementFactory(
        piid=piid, parent_award_id=paid, federal_action_obligation=-1100, submission_id=sub_id
    )
    ap_3_row_2 = AwardProcurementFactory(
        piid=piid.lower(), parent_award_id=paid.lower(), federal_action_obligation=-10, submission_id=sub_id
    )
    ap_3_row_3 = AwardProcurementFactory(
        piid=piid, parent_award_id=paid.upper(), federal_action_obligation=-12, submission_id=sub_id
    )

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
        submission=submission,
    )
    assert errors == 1

    # Hardcoded FREC test
    cgac = CGACFactory(cgac_code="005")
    frec = FRECFactory(frec_code="1100", cgac=cgac)
    sub_id = 4
    submission = SubmissionFactory(submission_id=sub_id, cgac_code=None, frec_code="1100")
    piid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    paid = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First piid/paid rows
    af_4_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        piid=piid.lower(),
        parent_award_id=paid.upper(),
        allocation_transfer_agency="256",
        agency_identifier="002",
        submission_id=sub_id,
    )
    af_4_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid,
        parent_award_id=paid,
        allocation_transfer_agency="256",
        agency_identifier="002",
        submission_id=sub_id,
    )
    # Ignored because it has a different ATA even though that ATA matches the FREC table
    af_4_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        piid=piid.upper(),
        parent_award_id=paid.lower(),
        allocation_transfer_agency="005",
        agency_identifier="002",
        submission_id=sub_id,
    )

    # Sum all of these should be equal to that of first piid/paid
    ap_4_row_1 = AwardProcurementFactory(
        piid=piid, parent_award_id=paid.lower(), federal_action_obligation=-1100, submission_id=sub_id
    )
    ap_4_row_2 = AwardProcurementFactory(
        piid=piid.lower(), parent_award_id=paid.upper(), federal_action_obligation=-10, submission_id=sub_id
    )
    ap_4_row_3 = AwardProcurementFactory(
        piid=piid, parent_award_id=paid, federal_action_obligation=-12, submission_id=sub_id
    )

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
        submission=submission,
    )
    assert errors == 1
