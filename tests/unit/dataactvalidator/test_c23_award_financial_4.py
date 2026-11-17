from random import choice
from string import ascii_uppercase, ascii_lowercase, digits
from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AwardFinancialFactory, AwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "c23_award_financial_4"


def test_column_headers(database):
    expected_subset = {
        "source_row_number",
        "source_value_uri",
        "source_value_transaction_obligated_amou_sum",
        "target_value_federal_action_obligation_sum",
        "target_value_original_loan_subsidy_cost_sum",
        "difference",
        "uniqueid_URI",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test for each unique URI in File C, the sum of each TransactionObligatedAmount should match (but with opposite
    signs) the sum of the FederalActionObligation or OriginalLoanSubsidyCost amounts reported in D2. This rule applies
    when the ATA and AID are the same, when the ATA is the submitting agency, or when the ATA is blank.
    """
    # Create a 12 character random uri
    uri_1 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    uri_2 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    uri_3 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    uri_4 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    sub_id = 1
    submission = SubmissionFactory(submission_id=sub_id, cgac_code="001")

    # Simple sum
    af_1_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100, uri=uri_1, allocation_transfer_agency=None, submission_id=sub_id
    )
    af_1_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11, uri=uri_1.lower(), allocation_transfer_agency=None, submission_id=sub_id
    )
    # Ignored row because it has non-matching ATA/AID but the rest of the sum works
    af_1_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=99,
        uri=uri_1,
        allocation_transfer_agency="good",
        agency_identifier="bad",
        submission_id=sub_id,
    )
    # Non-ignored rows with a matching ATA/AID
    af_2_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=9900, uri=uri_2, allocation_transfer_agency=None, submission_id=sub_id
    )
    af_2_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=99,
        uri=uri_2.lower(),
        allocation_transfer_agency="good",
        agency_identifier="good",
        submission_id=sub_id,
    )
    # Ignored row with non-matching ATA/AID
    af_3 = AwardFinancialFactory(
        transaction_obligated_amou=8888,
        uri=uri_3,
        allocation_transfer_agency="good",
        agency_identifier="bad",
        submission_id=sub_id,
    )
    # No TOA in File C, ignored
    af_4 = AwardFinancialFactory(
        transaction_obligated_amou=None,
        uri=uri_4.lower(),
        allocation_transfer_agency="good",
        agency_identifier="good",
        submission_id=sub_id,
    )

    # Correct sum
    afa_1_row_1 = AwardFinancialAssistanceFactory(
        uri=uri_1,
        federal_action_obligation=-1100,
        original_loan_subsidy_cost=None,
        record_type="1",
        submission_id=sub_id,
    )
    afa_1_row_2 = AwardFinancialAssistanceFactory(
        uri=uri_1, federal_action_obligation=-10, original_loan_subsidy_cost=None, record_type="1", submission_id=sub_id
    )
    # original loan subsidy cost used in this row because assistance type is '08'
    afa_1_row_3 = AwardFinancialAssistanceFactory(
        uri=uri_1,
        original_loan_subsidy_cost=-1,
        assistance_type="08",
        federal_action_obligation=None,
        record_type="1",
        submission_id=sub_id,
    )
    # federal action obligation used in this row (it's 0), because assistance type is not 07 and 08
    afa_1_row_4 = AwardFinancialAssistanceFactory(
        uri=uri_1,
        original_loan_subsidy_cost=-2222,
        assistance_type="f001",
        federal_action_obligation=None,
        record_type="1",
        submission_id=sub_id,
    )
    # Ignored because record type isn't 1
    afa_1_row_5 = AwardFinancialAssistanceFactory(
        uri=uri_1,
        federal_action_obligation=-1100,
        original_loan_subsidy_cost=None,
        record_type="2",
        submission_id=sub_id,
    )
    # Uri 2 Test for non-ignored ATA
    afa_2 = AwardFinancialAssistanceFactory(
        uri=uri_2,
        federal_action_obligation=-9999,
        original_loan_subsidy_cost=None,
        record_type="1",
        submission_id=sub_id,
    )
    # Uri 3 test for ignoring a non-matching ATA/AID
    afa_3 = AwardFinancialAssistanceFactory(
        uri=uri_3, federal_action_obligation=-9999, record_type="1", submission_id=sub_id
    )

    # This one matches but will be ignored
    afa_4 = AwardFinancialAssistanceFactory(uri=uri_4, federal_action_obligation=-9999, submission_id=sub_id)

    errors = number_of_errors(
        _FILE,
        database,
        models=[
            af_1_row_1,
            af_1_row_2,
            af_1_row_3,
            af_2_row_1,
            af_2_row_2,
            af_3,
            af_4,
            afa_1_row_1,
            afa_1_row_2,
            afa_1_row_3,
            afa_1_row_4,
            afa_1_row_5,
            afa_2,
            afa_3,
            afa_4,
        ],
        submission=submission,
    )
    assert errors == 0


def test_success_submitting_ata(database):
    """Test for each unique URI in File C, the sum of each TransactionObligatedAmount should match (but with opposite
    signs) the sum of the FederalActionObligation or OriginalLoanSubsidyCost amounts reported in D2. This rule applies
    when the ATA and AID are the same, when the ATA is the submitting agency, or when the ATA is blank.
    Test CGAC/FREC match the ATA
    """

    cgac = CGACFactory(cgac_code="001")
    frec = FRECFactory(frec_code="0001", cgac=cgac)

    # Basic CGAC test
    sub_id = 1
    submission = SubmissionFactory(submission_id=sub_id, cgac_code="001", frec_code=None)
    uri = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First uri rows
    af_1_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        uri=uri,
        allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id,
    )
    af_1_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        uri=uri.lower(),
        allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id,
    )
    # Ignored because it has a different ATA
    af_1_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        uri=uri.upper(),
        allocation_transfer_agency="003",
        agency_identifier="002",
        submission_id=sub_id,
    )

    # Sum all of these should be equal to that of first uri
    afa_1_row_1 = AwardFinancialAssistanceFactory(
        uri=uri.upper(), federal_action_obligation=-1100, assistance_type="F001", record_type="1", submission_id=sub_id
    )
    afa_1_row_2 = AwardFinancialAssistanceFactory(
        uri=uri, federal_action_obligation=-10, assistance_type="F001", record_type="1", submission_id=sub_id
    )
    afa_1_row_3 = AwardFinancialAssistanceFactory(
        uri=uri.lower(), federal_action_obligation=-1, assistance_type="F001", record_type="1", submission_id=sub_id
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
            afa_1_row_1,
            afa_1_row_2,
            afa_1_row_3,
        ],
        submission=submission,
    )
    assert errors == 0

    # FREC test, should align with the associated CGAC code
    sub_id = 2
    submission = SubmissionFactory(submission_id=sub_id, cgac_code=None, frec_code="0001")
    uri = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First uri rows
    af_2_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        uri=uri.lower(),
        allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id,
    )
    af_2_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        uri=uri.upper(),
        allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id,
    )
    # Ignored because it has a different ATA
    af_2_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        uri=uri,
        allocation_transfer_agency="003",
        agency_identifier="002",
        submission_id=sub_id,
    )

    # Sum all of these should be equal to that of first uri
    afa_2_row_1 = AwardFinancialAssistanceFactory(
        uri=uri, federal_action_obligation=-1100, assistance_type="F001", record_type="1", submission_id=sub_id
    )
    afa_2_row_2 = AwardFinancialAssistanceFactory(
        uri=uri.upper(), federal_action_obligation=-10, assistance_type="F001", record_type="1", submission_id=sub_id
    )
    afa_2_row_3 = AwardFinancialAssistanceFactory(
        uri=uri.lower(), federal_action_obligation=-1, assistance_type="F001", record_type="1", submission_id=sub_id
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
            afa_2_row_1,
            afa_2_row_2,
            afa_2_row_3,
        ],
        submission=submission,
    )
    assert errors == 0

    # Hardcoded CGAC test
    sub_id = 3
    submission = SubmissionFactory(submission_id=sub_id, cgac_code="097", frec_code=None)
    uri = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First uri rows
    af_3_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        uri=uri,
        allocation_transfer_agency="017",
        agency_identifier="002",
        submission_id=sub_id,
    )
    af_3_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        uri=uri.lower(),
        allocation_transfer_agency="057",
        agency_identifier="002",
        submission_id=sub_id,
    )
    # Ignored because it has a different ATA
    af_3_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        uri=uri.upper(),
        allocation_transfer_agency="003",
        agency_identifier="002",
        submission_id=sub_id,
    )

    # Sum all of these should be equal to that of first uri
    afa_3_row_1 = AwardFinancialAssistanceFactory(
        uri=uri.upper(), federal_action_obligation=-1100, assistance_type="F001", record_type="1", submission_id=sub_id
    )
    afa_3_row_2 = AwardFinancialAssistanceFactory(
        uri=uri.lower(), federal_action_obligation=-10, assistance_type="F001", record_type="1", submission_id=sub_id
    )
    afa_3_row_3 = AwardFinancialAssistanceFactory(
        uri=uri, federal_action_obligation=-1, assistance_type="F001", record_type="1", submission_id=sub_id
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
            afa_3_row_1,
            afa_3_row_2,
            afa_3_row_3,
        ],
        submission=submission,
    )
    assert errors == 0

    # Hardcoded FREC test
    cgac = CGACFactory(cgac_code="005")
    frec = FRECFactory(frec_code="1100", cgac=cgac)
    sub_id = 4
    submission = SubmissionFactory(submission_id=sub_id, cgac_code=None, frec_code="1100")
    uri = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First uri rows
    af_4_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        uri=uri,
        allocation_transfer_agency="256",
        agency_identifier="002",
        submission_id=sub_id,
    )
    af_4_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        uri=uri.lower(),
        allocation_transfer_agency="256",
        agency_identifier="002",
        submission_id=sub_id,
    )
    # Ignored because it has a different ATA even though that ATA matches the FREC table
    af_4_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        uri=uri.upper(),
        allocation_transfer_agency="005",
        agency_identifier="002",
        submission_id=sub_id,
    )

    # Sum all of these should be equal to that of first uri
    afa_4_row_1 = AwardFinancialAssistanceFactory(
        uri=uri.lower(), federal_action_obligation=-1100, assistance_type="F001", record_type="1", submission_id=sub_id
    )
    afa_4_row_2 = AwardFinancialAssistanceFactory(
        uri=uri.upper(), federal_action_obligation=-10, assistance_type="F001", record_type="1", submission_id=sub_id
    )
    afa_4_row_3 = AwardFinancialAssistanceFactory(
        uri=uri, federal_action_obligation=-1, assistance_type="F001", record_type="1", submission_id=sub_id
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
            afa_4_row_1,
            afa_4_row_2,
            afa_4_row_3,
        ],
        submission=submission,
    )
    assert errors == 0


def test_failure(database):
    """Test failure for each unique URI in File C, the sum of each TransactionObligatedAmount should match (but with
    opposite signs) the sum of the FederalActionObligation or OriginalLoanSubsidyCost amounts reported in D2. This rule
    applies when the ATA and AID are the same, when the ATA is the submitting agency, or when the ATA is blank.
    """
    # Create a 12 character random uri
    uri_1 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    uri_2 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    uri_3 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    uri_4 = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    sub_id = 1
    submission = SubmissionFactory(submission_id=sub_id, cgac_code="001")

    # Simple addition that doesn't add up right
    af_1_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100, uri=uri_1, allocation_transfer_agency=None, submission_id=sub_id
    )
    af_1_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=9, uri=uri_1.lower(), allocation_transfer_agency=None, submission_id=sub_id
    )
    # Ignore row that would make it add up right if it was counted because of non-matching ATA/AID
    af_1_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=1,
        uri=uri_1.lower(),
        allocation_transfer_agency="good",
        agency_identifier="bad",
        submission_id=sub_id,
    )
    # Incorrect addition based on assistance type in AFA
    af_2 = AwardFinancialFactory(
        transaction_obligated_amou=9999, uri=uri_2, allocation_transfer_agency=None, submission_id=sub_id
    )
    # Don't ignore when ATA and AID match
    af_3 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        uri=uri_3,
        allocation_transfer_agency="good",
        agency_identifier="good",
        submission_id=sub_id,
    )
    # Not ignored with TOA of 0
    af_4 = AwardFinancialFactory(
        transaction_obligated_amou=0,
        uri=uri_4,
        allocation_transfer_agency="good",
        agency_identifier="good",
        submission_id=sub_id,
    )

    # Sum of this uri doesn't add up to af uri sum
    afa_1_row_1 = AwardFinancialAssistanceFactory(
        uri=uri_1,
        federal_action_obligation=-1100,
        original_loan_subsidy_cost=None,
        record_type="1",
        submission_id=sub_id,
    )
    afa_1_row_2 = AwardFinancialAssistanceFactory(
        uri=uri_1, federal_action_obligation=-10, original_loan_subsidy_cost=None, record_type="1", submission_id=sub_id
    )
    # Both of these rows use the column that isn't filled in for summing so neither results in the correct number
    afa_2_row_1 = AwardFinancialAssistanceFactory(
        uri=uri_2,
        federal_action_obligation=-9999,
        original_loan_subsidy_cost=None,
        record_type="1",
        submission_id=sub_id,
    )
    afa_2_row_2 = AwardFinancialAssistanceFactory(
        uri=uri_2,
        federal_action_obligation=None,
        original_loan_subsidy_cost=-1000,
        assistance_type="07",
        record_type="1",
        submission_id=sub_id,
    )
    # This shouldn't be ignored
    afa_3 = AwardFinancialAssistanceFactory(
        uri=uri_3, federal_action_obligation=0, original_loan_subsidy_cost=None, record_type="1", submission_id=sub_id
    )
    # This shouldn't be ignored
    afa_4 = AwardFinancialAssistanceFactory(
        uri=uri_4, federal_action_obligation=1, original_loan_subsidy_cost=None, record_type="1", submission_id=sub_id
    )

    errors = number_of_errors(
        _FILE,
        database,
        models=[
            af_1_row_1,
            af_1_row_2,
            af_1_row_3,
            af_2,
            af_3,
            af_4,
            afa_1_row_1,
            afa_1_row_2,
            afa_2_row_1,
            afa_2_row_2,
            afa_3,
            afa_4,
        ],
        submission=submission,
    )
    assert errors == 4


def test_failure_submitting_ata(database):
    """Test failure for each unique URI in File C, the sum of each TransactionObligatedAmount should match (but with
    opposite signs) the sum of the FederalActionObligation or OriginalLoanSubsidyCost amounts reported in D2. This rule
    applies when the ATA and AID are the same, when the ATA is the submitting agency, or when the ATA is blank.
    Test CGAC/FREC match the ATA
    """

    cgac = CGACFactory(cgac_code="001")
    frec = FRECFactory(frec_code="0001", cgac=cgac)

    # Basic CGAC test
    sub_id = 1
    submission = SubmissionFactory(submission_id=sub_id, cgac_code="001", frec_code=None)
    uri = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First uri rows
    af_1_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        uri=uri.upper(),
        allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id,
    )
    af_1_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        uri=uri.lower(),
        allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id,
    )
    # Ignored because it has a different ATA
    af_1_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        uri=uri,
        allocation_transfer_agency="003",
        agency_identifier="002",
        submission_id=sub_id,
    )

    # These add up to the sum of all 3, including the one that was ignored
    afa_1_row_1 = AwardFinancialAssistanceFactory(
        uri=uri, federal_action_obligation=-1100, assistance_type="F001", record_type="1", submission_id=sub_id
    )
    afa_1_row_2 = AwardFinancialAssistanceFactory(
        uri=uri.upper(), federal_action_obligation=-10, assistance_type="F001", record_type="1", submission_id=sub_id
    )
    afa_1_row_3 = AwardFinancialAssistanceFactory(
        uri=uri.lower(), federal_action_obligation=-12, assistance_type="F001", record_type="1", submission_id=sub_id
    )

    errors = number_of_errors(
        _FILE,
        database,
        models=[
            af_1_row_1,
            af_1_row_2,
            af_1_row_3,
            afa_1_row_1,
            afa_1_row_2,
            afa_1_row_3,
        ],
        submission=submission,
    )
    assert errors == 1

    # FREC test, should align with the associated CGAC code
    sub_id = 2
    submission = SubmissionFactory(submission_id=sub_id, cgac_code=None, frec_code="0001")
    uri = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First uri rows
    af_2_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        uri=uri.lower(),
        allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id,
    )
    af_2_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        uri=uri,
        allocation_transfer_agency="001",
        agency_identifier="002",
        submission_id=sub_id,
    )
    # Ignored because it has a different ATA
    af_2_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        uri=uri.upper(),
        allocation_transfer_agency="003",
        agency_identifier="002",
        submission_id=sub_id,
    )

    # Sum all of these should be equal to that of first uri
    afa_2_row_1 = AwardFinancialAssistanceFactory(
        uri=uri, federal_action_obligation=-1100, assistance_type="F001", record_type="1", submission_id=sub_id
    )
    afa_2_row_2 = AwardFinancialAssistanceFactory(
        uri=uri.upper(), federal_action_obligation=-10, assistance_type="F001", record_type="1", submission_id=sub_id
    )
    afa_2_row_3 = AwardFinancialAssistanceFactory(
        uri=uri.lower(), federal_action_obligation=-12, assistance_type="F001", record_type="1", submission_id=sub_id
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
            afa_2_row_1,
            afa_2_row_2,
            afa_2_row_3,
        ],
        submission=submission,
    )
    assert errors == 1

    # Hardcoded CGAC test
    sub_id = 3
    submission = SubmissionFactory(submission_id=sub_id, cgac_code="097", frec_code=None)
    uri = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First uri rows
    af_3_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        uri=uri.lower(),
        allocation_transfer_agency="017",
        agency_identifier="002",
        submission_id=sub_id,
    )
    af_3_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        uri=uri.upper(),
        allocation_transfer_agency="057",
        agency_identifier="002",
        submission_id=sub_id,
    )
    # Ignored because it has a different ATA
    af_3_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        uri=uri,
        allocation_transfer_agency="003",
        agency_identifier="002",
        submission_id=sub_id,
    )

    # Sum all of these should be equal to that of first uri
    afa_3_row_1 = AwardFinancialAssistanceFactory(
        uri=uri, federal_action_obligation=-1100, assistance_type="F001", record_type="1", submission_id=sub_id
    )
    afa_3_row_2 = AwardFinancialAssistanceFactory(
        uri=uri.lower(), federal_action_obligation=-10, assistance_type="F001", record_type="1", submission_id=sub_id
    )
    afa_3_row_3 = AwardFinancialAssistanceFactory(
        uri=uri.upper(), federal_action_obligation=-12, assistance_type="F001", record_type="1", submission_id=sub_id
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
            afa_3_row_1,
            afa_3_row_2,
            afa_3_row_3,
        ],
        submission=submission,
    )
    assert errors == 1

    # Hardcoded FREC test
    cgac = CGACFactory(cgac_code="005")
    frec = FRECFactory(frec_code="1100", cgac=cgac)
    sub_id = 4
    submission = SubmissionFactory(submission_id=sub_id, cgac_code=None, frec_code="1100")
    uri = "".join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # First uri rows
    af_4_row_1 = AwardFinancialFactory(
        transaction_obligated_amou=1100,
        uri=uri.upper(),
        allocation_transfer_agency="256",
        agency_identifier="002",
        submission_id=sub_id,
    )
    af_4_row_2 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        uri=uri,
        allocation_transfer_agency="256",
        agency_identifier="002",
        submission_id=sub_id,
    )
    # Ignored because it has a different ATA even though that ATA matches the FREC table
    af_4_row_3 = AwardFinancialFactory(
        transaction_obligated_amou=11,
        uri=uri.lower(),
        allocation_transfer_agency="005",
        agency_identifier="002",
        submission_id=sub_id,
    )

    # Sum all of these should be equal to that of first uri
    afa_4_row_1 = AwardFinancialAssistanceFactory(
        uri=uri.lower(), federal_action_obligation=-1100, assistance_type="F001", record_type="1", submission_id=sub_id
    )
    afa_4_row_2 = AwardFinancialAssistanceFactory(
        uri=uri.upper(), federal_action_obligation=-10, assistance_type="F001", record_type="1", submission_id=sub_id
    )
    afa_4_row_3 = AwardFinancialAssistanceFactory(
        uri=uri, federal_action_obligation=-12, assistance_type="F001", record_type="1", submission_id=sub_id
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
            afa_4_row_1,
            afa_4_row_2,
            afa_4_row_3,
        ],
        submission=submission,
    )
    assert errors == 1
