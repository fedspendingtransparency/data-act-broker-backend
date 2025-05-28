from dataactcore.models.stagingModels import AwardFinancial
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "c28_award_financial_1"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "tas",
        "object_class",
        "program_activity_code",
        "program_activity_name",
        "by_direct_reimbursable_fun",
        "disaster_emergency_fund_code",
        "fain",
        "uri",
        "piid",
        "parent_award_id",
        "prior_year_adjustment",
        "uniqueid_TAS",
        "uniqueid_ProgramActivityCode",
        "uniqueid_ProgramActivityName",
        "uniqueid_ObjectClass",
        "uniqueid_ByDirectReimbursableFundingSource",
        "uniqueid_DisasterEmergencyFundCode",
        "uniqueid_FAIN",
        "uniqueid_URI",
        "uniqueid_PIID",
        "uniqueid_ParentAwardId",
        "uniqueid_PriorYearAdjustment",
    }
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """
    Tests the combination of TAS/object class/program activity code+name/reimbursable flag/DEFC/Award ID/PYA
    in File C (award financial) should be unique for USSGL-related balances.
    """

    af1 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="1",
        program_activity_code="1",
        program_activity_name="n",
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="n",
        fain="1",
        uri="1",
        piid="1",
        parent_award_id="1",
        transaction_obligated_amou=None,
        prior_year_adjustment="X",
    )

    af2 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="2",
        object_class="1",
        program_activity_code="1",
        program_activity_name="n",
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="n",
        fain="1",
        uri="1",
        piid="1",
        parent_award_id="1",
        transaction_obligated_amou=None,
        prior_year_adjustment="X",
    )

    af3 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="2",
        program_activity_code="1",
        program_activity_name="n",
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="n",
        fain="1",
        uri="1",
        piid="1",
        parent_award_id="1",
        transaction_obligated_amou=None,
        prior_year_adjustment="X",
    )

    af4 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="1",
        program_activity_code="2",
        program_activity_name="n",
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="n",
        fain="1",
        uri="1",
        piid="1",
        parent_award_id="1",
        transaction_obligated_amou=None,
        prior_year_adjustment="X",
    )

    af5 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="1",
        program_activity_code="1",
        program_activity_name="m",
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="n",
        fain="1",
        uri="1",
        piid="1",
        parent_award_id="1",
        transaction_obligated_amou=None,
        prior_year_adjustment="X",
    )

    af6 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="1",
        program_activity_code="1",
        program_activity_name="n",
        by_direct_reimbursable_fun="d",
        disaster_emergency_fund_code="n",
        fain="1",
        uri="1",
        piid="1",
        parent_award_id="1",
        transaction_obligated_amou=None,
        prior_year_adjustment="X",
    )

    af7 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="1",
        program_activity_code="1",
        program_activity_name="n",
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="n",
        fain="1",
        uri="1",
        piid="1",
        parent_award_id="1",
        transaction_obligated_amou=None,
        prior_year_adjustment="b",
    )

    # Same values but a different DEFC
    af8 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="1",
        program_activity_code="1",
        program_activity_name="n",
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="m",
        fain="1",
        uri="1",
        piid="1",
        parent_award_id="1",
        transaction_obligated_amou=None,
        prior_year_adjustment="X",
    )

    af9 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="1",
        program_activity_code="1",
        program_activity_name="n",
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="n",
        fain="2",
        uri="1",
        piid="1",
        parent_award_id="1",
        transaction_obligated_amou=None,
        prior_year_adjustment="X",
    )

    af10 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="1",
        program_activity_code="1",
        program_activity_name="n",
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="n",
        fain="1",
        uri="2",
        piid="1",
        parent_award_id="1",
        transaction_obligated_amou=None,
        prior_year_adjustment="X",
    )

    af11 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="1",
        program_activity_code="1",
        program_activity_name="n",
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="n",
        fain="1",
        uri="1",
        piid="2",
        parent_award_id="1",
        transaction_obligated_amou=None,
        prior_year_adjustment="X",
    )

    af12 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="1",
        program_activity_code="1",
        program_activity_name="n",
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="n",
        fain="1",
        uri="1",
        piid="1",
        parent_award_id="2",
        transaction_obligated_amou=None,
        prior_year_adjustment="X",
    )

    assert (
        number_of_errors(_FILE, database, models=[af1, af2, af3, af4, af5, af6, af7, af8, af9, af10, af11, af12]) == 0
    )


def test_ignore_toa(database):
    """
    Tests that all combinations of TAS/object class/program activity code+name/reimbursable flag/DEFC/Award ID/PYA
    in File C (award financial) are not unique for USSGL-related balances, ignoring when TOA is not NULL
    """

    af1 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="1",
        program_activity_code="1",
        program_activity_name="n",
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="n",
        fain="1",
        uri="1",
        piid="1",
        parent_award_id="1",
        transaction_obligated_amou=0,
        prior_year_adjustment="X",
    )

    af2 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="1",
        program_activity_code="1",
        program_activity_name="n",
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="n",
        fain="1",
        uri="1",
        piid="1",
        parent_award_id="1",
        transaction_obligated_amou=0,
        prior_year_adjustment="X",
    )

    assert number_of_errors(_FILE, database, models=[af1, af2]) == 0


def test_ignore_null_pacpan(database):
    """
    Tests that all combinations of TAS/object class/program activity code+name/reimbursable flag/DEFC/Award ID/PYA
    in File C (award financial) are not unique for USSGL-related balances, ignoring when PAC/PAN are null
    """

    af1 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="1",
        program_activity_code=None,
        program_activity_name="",
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="n",
        fain="1",
        uri="1",
        piid="1",
        parent_award_id="1",
        transaction_obligated_amou=None,
        prior_year_adjustment="X",
    )

    af2 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="1",
        program_activity_code="",
        program_activity_name=None,
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="n",
        fain="1",
        uri="1",
        piid="1",
        parent_award_id="1",
        transaction_obligated_amou=None,
        prior_year_adjustment="X",
    )

    assert number_of_errors(_FILE, database, models=[af1, af2]) == 0


def test_failure(database):
    """
    Tests that all combinations of TAS/object class/program activity code+name/reimbursable flag/DEFC/Award ID/PYA
    in File C (award financial) are not unique for USSGL-related balances.
    """

    af1 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="1",
        program_activity_code="1",
        program_activity_name="n",
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="n",
        fain="1",
        uri="1",
        piid="1",
        parent_award_id="1",
        transaction_obligated_amou=None,
        prior_year_adjustment="X",
    )

    af2 = AwardFinancial(
        job_id=1,
        row_number=1,
        display_tas="1",
        object_class="1",
        program_activity_code="1",
        program_activity_name="n",
        by_direct_reimbursable_fun="r",
        disaster_emergency_fund_code="N",
        fain="1",
        uri="1",
        piid="1",
        parent_award_id="1",
        transaction_obligated_amou=None,
        prior_year_adjustment="x",
    )

    assert number_of_errors(_FILE, database, models=[af1, af2]) == 1
