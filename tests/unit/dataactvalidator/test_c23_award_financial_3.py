from random import choice
from string import ascii_uppercase, ascii_lowercase, digits
from tests.unit.dataactcore.factories.staging import AwardFinancialFactory, AwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c23_award_financial_3'


def test_column_headers(database):
    expected_subset = {'source_row_number', 'source_value_fain', 'source_value_transaction_obligated_amou_sum',
                       'target_value_federal_action_obligation_sum', 'target_value_original_loan_subsidy_cost_sum',
                       'difference', 'uniqueid_FAIN'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test for each unique FAIN in File C, the sum of each TransactionObligatedAmount should match (but with opposite
        signs) the sum of the FederalActionObligation or OriginalLoanSubsidyCost amounts reported in D2. This rule does
        not apply if the ATA field is populated and is different from the Agency ID.
    """
    # Create a 12 character random fain
    fain_1 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    fain_2 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    fain_3 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # Just some basic sums to make sure it works
    af_1_row_1 = AwardFinancialFactory(transaction_obligated_amou=1100, fain=fain_1, allocation_transfer_agency=None)
    af_1_row_2 = AwardFinancialFactory(transaction_obligated_amou=11, fain=fain_1.lower(),
                                       allocation_transfer_agency=None)
    # Non-ignored rows with a matching ATA/AID
    af_2_row_1 = AwardFinancialFactory(transaction_obligated_amou=9900, fain=fain_2, allocation_transfer_agency=None)
    af_2_row_2 = AwardFinancialFactory(transaction_obligated_amou=99, fain=fain_2, allocation_transfer_agency='good',
                                       agency_identifier='good')
    # Ignored row with non-matching ATA/AID
    af_3 = AwardFinancialFactory(transaction_obligated_amou=8888, fain=fain_3, allocation_transfer_agency='good',
                                 agency_identifier='bad')

    # Fain sums for AFA
    afa_1_row_1 = AwardFinancialAssistanceFactory(fain=fain_1, federal_action_obligation=-1100,
                                                  original_loan_subsidy_cost=None)
    afa_1_row_2 = AwardFinancialAssistanceFactory(fain=fain_1.lower(), federal_action_obligation=-10,
                                                  original_loan_subsidy_cost=None)
    # original loan subsidy cost used in this row because assistance type is '08'
    afa_1_row_3 = AwardFinancialAssistanceFactory(fain=fain_1, original_loan_subsidy_cost=-1, assistance_type='08',
                                                  federal_action_obligation=None)
    # federal action obligation used in this row (it's 0), because assistance type is not 07 and 08
    afa_1_row_4 = AwardFinancialAssistanceFactory(fain=fain_1, original_loan_subsidy_cost=-2222, assistance_type='09',
                                                  federal_action_obligation=None)
    # Fain 2 Test for non-ignored ATA
    afa_2 = AwardFinancialAssistanceFactory(fain=fain_2, federal_action_obligation=-9999,
                                            original_loan_subsidy_cost=None)
    # Fain 3 test for ignoring a non-matching ATA/AID
    afa_3 = AwardFinancialAssistanceFactory(fain=fain_3, federal_action_obligation=-9999)

    errors = number_of_errors(_FILE, database, models=[af_1_row_1, af_1_row_2, af_2_row_1, af_2_row_2, af_3,
                                                       afa_1_row_1, afa_1_row_2, afa_1_row_3, afa_1_row_4, afa_2,
                                                       afa_3])
    assert errors == 0


def test_failure(database):
    """ Test failure for each unique FAIN in File C, the sum of each TransactionObligatedAmount should match (but with
        opposite signs) the sum of the FederalActionObligation or OriginalLoanSubsidyCost amounts reported in D2. This
        rule does not apply if the ATA field is populated and is different from the Agency ID.
    """
    # Create a 12 character random fain
    fain_1 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    fain_2 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))
    fain_3 = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for _ in range(12))

    # Simple addition that doesn't add up right
    af_1_row_1 = AwardFinancialFactory(transaction_obligated_amou=1100, fain=fain_1, allocation_transfer_agency=None)
    af_1_row_2 = AwardFinancialFactory(transaction_obligated_amou=11, fain=fain_1.lower(),
                                       allocation_transfer_agency=None)
    # Incorrect addition based on assistance type in AFA
    af_2 = AwardFinancialFactory(transaction_obligated_amou=9999, fain=fain_2, allocation_transfer_agency=None)
    # Don't ignore when ATA and AID match
    af_3 = AwardFinancialFactory(transaction_obligated_amou=1111, fain=fain_3, allocation_transfer_agency='good',
                                 agency_identifier='good')

    # Sum of this fain doesn't add up to af fain sum
    afa_1_row_1 = AwardFinancialAssistanceFactory(fain=fain_1, federal_action_obligation=-1100,
                                                  original_loan_subsidy_cost=None)
    afa_1_row_2 = AwardFinancialAssistanceFactory(fain=fain_1.lower(), federal_action_obligation=-10,
                                                  original_loan_subsidy_cost=None)
    # Both of these rows use the column that isn't filled in for summing so neither results in the correct number
    afa_2_row_1 = AwardFinancialAssistanceFactory(fain=fain_2, federal_action_obligation=-9999,
                                                  original_loan_subsidy_cost=None)
    afa_2_row_2 = AwardFinancialAssistanceFactory(fain=fain_2, federal_action_obligation=None,
                                                  original_loan_subsidy_cost=-9999, assistance_type='07')
    # This shouldn't be ignored
    afa_3 = AwardFinancialAssistanceFactory(fain=fain_3, federal_action_obligation=0, original_loan_subsidy_cost=None)

    errors = number_of_errors(_FILE, database, models=[af_1_row_1, af_1_row_2, af_2, af_3, afa_1_row_1, afa_1_row_2,
                                                       afa_2_row_1, afa_2_row_2, afa_3])
    assert errors == 3
