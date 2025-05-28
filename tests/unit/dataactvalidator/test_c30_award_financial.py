from random import randint

from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "c30_award_financial"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "ussgl487200_downward_adjus_cpe",
        "ussgl497200_downward_adjus_cpe",
        "uniqueid_TAS",
        "uniqueid_DisasterEmergencyFundCode",
        "uniqueid_PIID",
        "uniqueid_FAIN",
        "uniqueid_URI",
    }
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success_pre_25(database):
    """Tests beginning in FY25, if the row is a non-TOA (balance) row, then
    USSGL487200_DownwardAdjustmentsOfPriorYearPrepaidAdvancedUndeliveredOrdersObligationsRefundsCollected_CPE and
    USSGL497200_DownwardAdjustmentsOfPriorYearPaidDeliveredOrdersObligationsRefundsCollected_CPE cannot be blank.
    FY before 25, should just be ignored
    """

    sub_id = randint(1000, 10000)
    submission = SubmissionFactory(submission_id=sub_id, reporting_fiscal_year=2024)

    # Doesn't matter what it looks like, they're all passing because pre-2025
    af1 = AwardFinancialFactory(
        submission_id=sub_id,
        ussgl487200_downward_adjus_cpe=5,
        ussgl497200_downward_adjus_cpe=None,
        transaction_obligated_amou=None,
    )
    af2 = AwardFinancialFactory(
        submission_id=sub_id,
        ussgl487200_downward_adjus_cpe=None,
        ussgl497200_downward_adjus_cpe=4,
        transaction_obligated_amou=None,
    )
    af3 = AwardFinancialFactory(
        submission_id=sub_id,
        ussgl487200_downward_adjus_cpe=None,
        ussgl497200_downward_adjus_cpe=None,
        transaction_obligated_amou=None,
    )
    af4 = AwardFinancialFactory(
        submission_id=sub_id,
        ussgl487200_downward_adjus_cpe=5,
        ussgl497200_downward_adjus_cpe=2,
        transaction_obligated_amou=6,
    )

    assert number_of_errors(_FILE, database, models=[af1, af2, af3, af4], submission=submission) == 0


def test_success_post_25(database):
    """Tests beginning in FY25, if the row is a non-TOA (balance) row, then
    USSGL487200_DownwardAdjustmentsOfPriorYearPrepaidAdvancedUndeliveredOrdersObligationsRefundsCollected_CPE and
    USSGL497200_DownwardAdjustmentsOfPriorYearPaidDeliveredOrdersObligationsRefundsCollected_CPE cannot be blank.
    FY 25 and later if they're all filled it should pass
    """

    sub_id = randint(1000, 10000)
    submission = SubmissionFactory(submission_id=sub_id, reporting_fiscal_year=2024)

    # All contain data
    af1 = AwardFinancialFactory(
        submission_id=sub_id,
        ussgl487200_downward_adjus_cpe=5,
        ussgl497200_downward_adjus_cpe=4,
        transaction_obligated_amou=3,
    )

    assert number_of_errors(_FILE, database, models=[af1], submission=submission) == 0


def test_failure(database):
    """Tests failure beginning in FY25, if the row is a non-TOA (balance) row, then
    USSGL487200_DownwardAdjustmentsOfPriorYearPrepaidAdvancedUndeliveredOrdersObligationsRefundsCollected_CPE and
    USSGL497200_DownwardAdjustmentsOfPriorYearPaidDeliveredOrdersObligationsRefundsCollected_CPE cannot be blank.
    """

    sub_id = randint(1000, 10000)
    submission = SubmissionFactory(submission_id=sub_id, reporting_fiscal_year=2025)

    # In all cases, at least one of the ussgl values is blank when transaction_obligated_amount is blank
    af1 = AwardFinancialFactory(
        submission_id=sub_id,
        ussgl487200_downward_adjus_cpe=5,
        ussgl497200_downward_adjus_cpe=None,
        transaction_obligated_amou=None,
    )
    af2 = AwardFinancialFactory(
        submission_id=sub_id,
        ussgl487200_downward_adjus_cpe=None,
        ussgl497200_downward_adjus_cpe=4,
        transaction_obligated_amou=None,
    )
    af3 = AwardFinancialFactory(
        submission_id=sub_id,
        ussgl487200_downward_adjus_cpe=None,
        ussgl497200_downward_adjus_cpe=None,
        transaction_obligated_amou=None,
    )

    assert number_of_errors(_FILE, database, models=[af1, af2, af3], submission=submission) == 3
