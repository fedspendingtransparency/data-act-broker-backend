from random import randint

from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c20_award_financial'
_TAS = 'c20_award_financial_tas'


def test_column_headers(database):
    expected_subset = {'row_number'}
    actual = set(query_columns(_FILE, database.stagingDb))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that the sum of financial elements in File C is less than or equal
     to the corresponding element in File B for the same TAS and Object Class combination"""
    submission_id = randint(1000, 10000)
    tas, object_class = 'some-tas', 'some-class'
    ussgl480100_undelivered_or_fyb = randint(-10000, -1000)
    ussgl480100_undelivered_or_cpe = randint(-10000, -1000)
    ussgl483100_undelivered_or_cpe = randint(-10000, -1000)
    ussgl488100_upward_adjustm_cpe = randint(-10000, -1000)
    obligations_undelivered_or_fyb = randint(-10000, -1000)
    obligations_undelivered_or_cpe = randint(-10000, -1000)
    ussgl490100_delivered_orde_fyb = randint(-10000, -1000)
    ussgl490100_delivered_orde_cpe = randint(-10000, -1000)
    ussgl493100_delivered_orde_cpe = randint(-10000, -1000)
    ussgl498100_upward_adjustm_cpe = randint(-10000, -1000)
    obligations_delivered_orde_fyb = randint(-10000, -1000)
    obligations_delivered_orde_cpe = randint(-10000, -1000)
    ussgl480200_undelivered_or_fyb = randint(-10000, -1000)
    ussgl480200_undelivered_or_cpe = randint(-10000, -1000)
    ussgl483200_undelivered_or_cpe = randint(-10000, -1000)
    ussgl488200_upward_adjustm_cpe = randint(-10000, -1000)
    gross_outlays_undelivered_fyb = randint(-10000, -1000)
    gross_outlays_undelivered_cpe = randint(-10000, -1000)
    ussgl490200_delivered_orde_cpe = randint(-10000, -1000)
    ussgl490800_authority_outl_fyb = randint(-10000, -1000)
    ussgl490800_authority_outl_cpe = randint(-10000, -1000)
    ussgl498200_upward_adjustm_cpe = randint(-10000, -1000)
    gross_outlays_delivered_or_fyb = randint(-10000, -1000)
    gross_outlays_delivered_or_cpe = randint(-10000, -1000)
    gross_outlay_amount_by_awa_fyb = randint(-10000, -1000)
    gross_outlay_amount_by_awa_cpe = randint(-10000, -1000)
    obligations_incurred_byawa_cpe = randint(-10000, -1000)
    ussgl487100_downward_adjus_cpe = randint(-10000, -1000)
    ussgl497100_downward_adjus_cpe = randint(-10000, -1000)
    ussgl487200_downward_adjus_cpe = randint(-10000, -1000)
    ussgl497200_downward_adjus_cpe = randint(-10000, -1000)
    deobligations_recov_by_awa_cpe = randint(-10000, -1000)

    af1 = AwardFinancialFactory(
        ussgl480100_undelivered_or_fyb = ussgl480100_undelivered_or_fyb,
        ussgl480100_undelivered_or_cpe = ussgl480100_undelivered_or_cpe,
        ussgl483100_undelivered_or_cpe = ussgl483100_undelivered_or_cpe,
        ussgl488100_upward_adjustm_cpe = ussgl488100_upward_adjustm_cpe,
        obligations_undelivered_or_fyb = obligations_undelivered_or_fyb,
        obligations_undelivered_or_cpe = obligations_undelivered_or_cpe,
        ussgl490100_delivered_orde_fyb = ussgl490100_delivered_orde_fyb,
        ussgl490100_delivered_orde_cpe = ussgl490100_delivered_orde_cpe,
        ussgl493100_delivered_orde_cpe = ussgl493100_delivered_orde_cpe,
        ussgl498100_upward_adjustm_cpe = ussgl498100_upward_adjustm_cpe,
        obligations_delivered_orde_fyb = obligations_delivered_orde_fyb,
        obligations_delivered_orde_cpe = obligations_delivered_orde_cpe,
        ussgl480200_undelivered_or_fyb = ussgl480200_undelivered_or_fyb,
        ussgl480200_undelivered_or_cpe = ussgl480200_undelivered_or_cpe,
        ussgl483200_undelivered_or_cpe = ussgl483200_undelivered_or_cpe,
        ussgl488200_upward_adjustm_cpe = ussgl488200_upward_adjustm_cpe,
        gross_outlays_undelivered_fyb = gross_outlays_undelivered_fyb,
        gross_outlays_undelivered_cpe = gross_outlays_undelivered_cpe,
        ussgl490200_delivered_orde_cpe = ussgl490200_delivered_orde_cpe,
        ussgl490800_authority_outl_fyb = ussgl490800_authority_outl_fyb,
        ussgl490800_authority_outl_cpe = ussgl490800_authority_outl_cpe,
        ussgl498200_upward_adjustm_cpe = ussgl498200_upward_adjustm_cpe,
        gross_outlays_delivered_or_fyb = gross_outlays_delivered_or_fyb,
        gross_outlays_delivered_or_cpe = gross_outlays_delivered_or_cpe,
        gross_outlay_amount_by_awa_fyb = gross_outlay_amount_by_awa_fyb,
        gross_outlay_amount_by_awa_cpe = gross_outlay_amount_by_awa_cpe,
        obligations_incurred_byawa_cpe = obligations_incurred_byawa_cpe,
        ussgl487100_downward_adjus_cpe = ussgl487100_downward_adjus_cpe,
        ussgl497100_downward_adjus_cpe = ussgl497100_downward_adjus_cpe,
        ussgl487200_downward_adjus_cpe = ussgl487200_downward_adjus_cpe,
        ussgl497200_downward_adjus_cpe = ussgl497200_downward_adjus_cpe,
        deobligations_recov_by_awa_cpe = deobligations_recov_by_awa_cpe,
        tas=tas,
        object_class=object_class,
        submission_id=submission_id
    )

    af2 = AwardFinancialFactory(
        ussgl480100_undelivered_or_fyb=ussgl480100_undelivered_or_fyb,
        ussgl480100_undelivered_or_cpe=ussgl480100_undelivered_or_cpe,
        ussgl483100_undelivered_or_cpe=ussgl483100_undelivered_or_cpe,
        ussgl488100_upward_adjustm_cpe=ussgl488100_upward_adjustm_cpe,
        obligations_undelivered_or_fyb=obligations_undelivered_or_fyb,
        obligations_undelivered_or_cpe=obligations_undelivered_or_cpe,
        ussgl490100_delivered_orde_fyb=ussgl490100_delivered_orde_fyb,
        ussgl490100_delivered_orde_cpe=ussgl490100_delivered_orde_cpe,
        ussgl493100_delivered_orde_cpe=ussgl493100_delivered_orde_cpe,
        ussgl498100_upward_adjustm_cpe=ussgl498100_upward_adjustm_cpe,
        obligations_delivered_orde_fyb=obligations_delivered_orde_fyb,
        obligations_delivered_orde_cpe=obligations_delivered_orde_cpe,
        ussgl480200_undelivered_or_fyb=ussgl480200_undelivered_or_fyb,
        ussgl480200_undelivered_or_cpe=ussgl480200_undelivered_or_cpe,
        ussgl483200_undelivered_or_cpe=ussgl483200_undelivered_or_cpe,
        ussgl488200_upward_adjustm_cpe=ussgl488200_upward_adjustm_cpe,
        gross_outlays_undelivered_fyb=gross_outlays_undelivered_fyb,
        gross_outlays_undelivered_cpe=gross_outlays_undelivered_cpe,
        ussgl490200_delivered_orde_cpe=ussgl490200_delivered_orde_cpe,
        ussgl490800_authority_outl_fyb=ussgl490800_authority_outl_fyb,
        ussgl490800_authority_outl_cpe=ussgl490800_authority_outl_cpe,
        ussgl498200_upward_adjustm_cpe=ussgl498200_upward_adjustm_cpe,
        gross_outlays_delivered_or_fyb=gross_outlays_delivered_or_fyb,
        gross_outlays_delivered_or_cpe=gross_outlays_delivered_or_cpe,
        gross_outlay_amount_by_awa_fyb=gross_outlay_amount_by_awa_fyb,
        gross_outlay_amount_by_awa_cpe=gross_outlay_amount_by_awa_cpe,
        obligations_incurred_byawa_cpe=obligations_incurred_byawa_cpe,
        ussgl487100_downward_adjus_cpe=ussgl487100_downward_adjus_cpe,
        ussgl497100_downward_adjus_cpe=ussgl497100_downward_adjus_cpe,
        ussgl487200_downward_adjus_cpe=ussgl487200_downward_adjus_cpe,
        ussgl497200_downward_adjus_cpe=ussgl497200_downward_adjus_cpe,
        deobligations_recov_by_awa_cpe=deobligations_recov_by_awa_cpe,
        tas=tas,
        object_class=object_class,
        submission_id=submission_id
    )

    op = ObjectClassProgramActivityFactory(
        ussgl480100_undelivered_or_fyb=ussgl480100_undelivered_or_fyb * 2,
        ussgl480100_undelivered_or_cpe=ussgl480100_undelivered_or_cpe * 2,
        ussgl483100_undelivered_or_cpe=ussgl483100_undelivered_or_cpe * 2,
        ussgl488100_upward_adjustm_cpe=ussgl488100_upward_adjustm_cpe * 2,
        obligations_undelivered_or_fyb=obligations_undelivered_or_fyb * 2,
        obligations_undelivered_or_cpe=obligations_undelivered_or_cpe * 2,
        ussgl490100_delivered_orde_fyb=ussgl490100_delivered_orde_fyb * 2,
        ussgl490100_delivered_orde_cpe=ussgl490100_delivered_orde_cpe * 2,
        ussgl493100_delivered_orde_cpe=ussgl493100_delivered_orde_cpe * 2,
        ussgl498100_upward_adjustm_cpe=ussgl498100_upward_adjustm_cpe * 2,
        obligations_delivered_orde_fyb=obligations_delivered_orde_fyb * 2,
        obligations_delivered_orde_cpe=obligations_delivered_orde_cpe * 2,
        ussgl480200_undelivered_or_fyb=ussgl480200_undelivered_or_fyb * 2,
        ussgl480200_undelivered_or_cpe=ussgl480200_undelivered_or_cpe * 2,
        ussgl483200_undelivered_or_cpe=ussgl483200_undelivered_or_cpe * 2,
        ussgl488200_upward_adjustm_cpe=ussgl488200_upward_adjustm_cpe * 2,
        gross_outlays_undelivered_fyb=gross_outlays_undelivered_fyb * 2,
        gross_outlays_undelivered_cpe=gross_outlays_undelivered_cpe * 2,
        ussgl490200_delivered_orde_cpe=ussgl490200_delivered_orde_cpe * 2,
        ussgl490800_authority_outl_fyb=ussgl490800_authority_outl_fyb * 2,
        ussgl490800_authority_outl_cpe=ussgl490800_authority_outl_cpe * 2,
        ussgl498200_upward_adjustm_cpe=ussgl498200_upward_adjustm_cpe * 2,
        gross_outlays_delivered_or_fyb=gross_outlays_delivered_or_fyb * 2,
        gross_outlays_delivered_or_cpe=gross_outlays_delivered_or_cpe * 2,
        gross_outlay_amount_by_pro_fyb=gross_outlay_amount_by_awa_fyb * 2,
        gross_outlay_amount_by_pro_cpe=gross_outlay_amount_by_awa_cpe * 2,
        obligations_incurred_by_pr_cpe=obligations_incurred_byawa_cpe * 2,
        ussgl487100_downward_adjus_cpe=ussgl487100_downward_adjus_cpe * 2,
        ussgl497100_downward_adjus_cpe=ussgl497100_downward_adjus_cpe * 2,
        ussgl487200_downward_adjus_cpe=ussgl487200_downward_adjus_cpe * 2,
        ussgl497200_downward_adjus_cpe=ussgl497200_downward_adjus_cpe * 2,
        deobligations_recov_by_pro_cpe=deobligations_recov_by_awa_cpe * 2,
        tas=tas,
        object_class=object_class,
        submission_id=submission_id
    )

    errors = number_of_errors(_FILE, database.stagingDb, models=[af1, af2, op])
    assert errors == 0


def test_failure(database):
    """ Tests that the sum of financial elements in File C is not less than or equal
         to the corresponding element in File B for the same TAS and Object Class combination"""
    submission_id = randint(1000, 10000)
    tas, object_class = 'some-tas', 'some-class'
    ussgl480100_undelivered_or_fyb = randint(-10000, -1000)
    ussgl480100_undelivered_or_cpe = randint(-10000, -1000)
    ussgl483100_undelivered_or_cpe = randint(-10000, -1000)
    ussgl488100_upward_adjustm_cpe = randint(-10000, -1000)
    obligations_undelivered_or_fyb = randint(-10000, -1000)
    obligations_undelivered_or_cpe = randint(-10000, -1000)
    ussgl490100_delivered_orde_fyb = randint(-10000, -1000)
    ussgl490100_delivered_orde_cpe = randint(-10000, -1000)
    ussgl493100_delivered_orde_cpe = randint(-10000, -1000)
    ussgl498100_upward_adjustm_cpe = randint(-10000, -1000)
    obligations_delivered_orde_fyb = randint(-10000, -1000)
    obligations_delivered_orde_cpe = randint(-10000, -1000)
    ussgl480200_undelivered_or_fyb = randint(-10000, -1000)
    ussgl480200_undelivered_or_cpe = randint(-10000, -1000)
    ussgl483200_undelivered_or_cpe = randint(-10000, -1000)
    ussgl488200_upward_adjustm_cpe = randint(-10000, -1000)
    gross_outlays_undelivered_fyb = randint(-10000, -1000)
    gross_outlays_undelivered_cpe = randint(-10000, -1000)
    ussgl490200_delivered_orde_cpe = randint(-10000, -1000)
    ussgl490800_authority_outl_fyb = randint(-10000, -1000)
    ussgl490800_authority_outl_cpe = randint(-10000, -1000)
    ussgl498200_upward_adjustm_cpe = randint(-10000, -1000)
    gross_outlays_delivered_or_fyb = randint(-10000, -1000)
    gross_outlays_delivered_or_cpe = randint(-10000, -1000)
    gross_outlay_amount_by_awa_fyb = randint(-10000, -1000)
    gross_outlay_amount_by_awa_cpe = randint(-10000, -1000)
    obligations_incurred_byawa_cpe = randint(-10000, -1000)
    ussgl487100_downward_adjus_cpe = randint(-10000, -1000)
    ussgl497100_downward_adjus_cpe = randint(-10000, -1000)
    ussgl487200_downward_adjus_cpe = randint(-10000, -1000)
    ussgl497200_downward_adjus_cpe = randint(-10000, -1000)
    deobligations_recov_by_awa_cpe = randint(-10000, -1000)

    af1 = AwardFinancialFactory(
        ussgl480100_undelivered_or_fyb=ussgl480100_undelivered_or_fyb,
        ussgl480100_undelivered_or_cpe=ussgl480100_undelivered_or_cpe,
        ussgl483100_undelivered_or_cpe=ussgl483100_undelivered_or_cpe,
        ussgl488100_upward_adjustm_cpe=ussgl488100_upward_adjustm_cpe,
        obligations_undelivered_or_fyb=obligations_undelivered_or_fyb,
        obligations_undelivered_or_cpe=obligations_undelivered_or_cpe,
        ussgl490100_delivered_orde_fyb=ussgl490100_delivered_orde_fyb,
        ussgl490100_delivered_orde_cpe=ussgl490100_delivered_orde_cpe,
        ussgl493100_delivered_orde_cpe=ussgl493100_delivered_orde_cpe,
        ussgl498100_upward_adjustm_cpe=ussgl498100_upward_adjustm_cpe,
        obligations_delivered_orde_fyb=obligations_delivered_orde_fyb,
        obligations_delivered_orde_cpe=obligations_delivered_orde_cpe,
        ussgl480200_undelivered_or_fyb=ussgl480200_undelivered_or_fyb,
        ussgl480200_undelivered_or_cpe=ussgl480200_undelivered_or_cpe,
        ussgl483200_undelivered_or_cpe=ussgl483200_undelivered_or_cpe,
        ussgl488200_upward_adjustm_cpe=ussgl488200_upward_adjustm_cpe,
        gross_outlays_undelivered_fyb=gross_outlays_undelivered_fyb,
        gross_outlays_undelivered_cpe=gross_outlays_undelivered_cpe,
        ussgl490200_delivered_orde_cpe=ussgl490200_delivered_orde_cpe,
        ussgl490800_authority_outl_fyb=ussgl490800_authority_outl_fyb,
        ussgl490800_authority_outl_cpe=ussgl490800_authority_outl_cpe,
        ussgl498200_upward_adjustm_cpe=ussgl498200_upward_adjustm_cpe,
        gross_outlays_delivered_or_fyb=gross_outlays_delivered_or_fyb,
        gross_outlays_delivered_or_cpe=gross_outlays_delivered_or_cpe,
        gross_outlay_amount_by_awa_fyb=gross_outlay_amount_by_awa_fyb,
        gross_outlay_amount_by_awa_cpe=gross_outlay_amount_by_awa_cpe,
        obligations_incurred_byawa_cpe=obligations_incurred_byawa_cpe,
        ussgl487100_downward_adjus_cpe=ussgl487100_downward_adjus_cpe,
        ussgl497100_downward_adjus_cpe=ussgl497100_downward_adjus_cpe,
        ussgl487200_downward_adjus_cpe=ussgl487200_downward_adjus_cpe,
        ussgl497200_downward_adjus_cpe=ussgl497200_downward_adjus_cpe,
        deobligations_recov_by_awa_cpe=deobligations_recov_by_awa_cpe,
        tas=tas,
        object_class=object_class,
        submission_id=submission_id
    )

    af2 = AwardFinancialFactory(
        ussgl480100_undelivered_or_fyb=ussgl480100_undelivered_or_fyb,
        ussgl480100_undelivered_or_cpe=ussgl480100_undelivered_or_cpe,
        ussgl483100_undelivered_or_cpe=ussgl483100_undelivered_or_cpe,
        ussgl488100_upward_adjustm_cpe=ussgl488100_upward_adjustm_cpe,
        obligations_undelivered_or_fyb=obligations_undelivered_or_fyb,
        obligations_undelivered_or_cpe=obligations_undelivered_or_cpe,
        ussgl490100_delivered_orde_fyb=ussgl490100_delivered_orde_fyb,
        ussgl490100_delivered_orde_cpe=ussgl490100_delivered_orde_cpe,
        ussgl493100_delivered_orde_cpe=ussgl493100_delivered_orde_cpe,
        ussgl498100_upward_adjustm_cpe=ussgl498100_upward_adjustm_cpe,
        obligations_delivered_orde_fyb=obligations_delivered_orde_fyb,
        obligations_delivered_orde_cpe=obligations_delivered_orde_cpe,
        ussgl480200_undelivered_or_fyb=ussgl480200_undelivered_or_fyb,
        ussgl480200_undelivered_or_cpe=ussgl480200_undelivered_or_cpe,
        ussgl483200_undelivered_or_cpe=ussgl483200_undelivered_or_cpe,
        ussgl488200_upward_adjustm_cpe=ussgl488200_upward_adjustm_cpe,
        gross_outlays_undelivered_fyb=gross_outlays_undelivered_fyb,
        gross_outlays_undelivered_cpe=gross_outlays_undelivered_cpe,
        ussgl490200_delivered_orde_cpe=ussgl490200_delivered_orde_cpe,
        ussgl490800_authority_outl_fyb=ussgl490800_authority_outl_fyb,
        ussgl490800_authority_outl_cpe=ussgl490800_authority_outl_cpe,
        ussgl498200_upward_adjustm_cpe=ussgl498200_upward_adjustm_cpe,
        gross_outlays_delivered_or_fyb=gross_outlays_delivered_or_fyb,
        gross_outlays_delivered_or_cpe=gross_outlays_delivered_or_cpe,
        gross_outlay_amount_by_awa_fyb=gross_outlay_amount_by_awa_fyb,
        gross_outlay_amount_by_awa_cpe=gross_outlay_amount_by_awa_cpe,
        obligations_incurred_byawa_cpe=obligations_incurred_byawa_cpe,
        ussgl487100_downward_adjus_cpe=ussgl487100_downward_adjus_cpe,
        ussgl497100_downward_adjus_cpe=ussgl497100_downward_adjus_cpe,
        ussgl487200_downward_adjus_cpe=ussgl487200_downward_adjus_cpe,
        ussgl497200_downward_adjus_cpe=ussgl497200_downward_adjus_cpe,
        deobligations_recov_by_awa_cpe=deobligations_recov_by_awa_cpe,
        tas=tas,
        object_class=object_class,
        submission_id=submission_id
    )

    op = ObjectClassProgramActivityFactory(
        ussgl480100_undelivered_or_fyb=ussgl480100_undelivered_or_fyb,
        ussgl480100_undelivered_or_cpe=ussgl480100_undelivered_or_cpe,
        ussgl483100_undelivered_or_cpe=ussgl483100_undelivered_or_cpe,
        ussgl488100_upward_adjustm_cpe=ussgl488100_upward_adjustm_cpe,
        obligations_undelivered_or_fyb=obligations_undelivered_or_fyb,
        obligations_undelivered_or_cpe=obligations_undelivered_or_cpe,
        ussgl490100_delivered_orde_fyb=ussgl490100_delivered_orde_fyb,
        ussgl490100_delivered_orde_cpe=ussgl490100_delivered_orde_cpe,
        ussgl493100_delivered_orde_cpe=ussgl493100_delivered_orde_cpe,
        ussgl498100_upward_adjustm_cpe=ussgl498100_upward_adjustm_cpe,
        obligations_delivered_orde_fyb=obligations_delivered_orde_fyb,
        obligations_delivered_orde_cpe=obligations_delivered_orde_cpe,
        ussgl480200_undelivered_or_fyb=ussgl480200_undelivered_or_fyb,
        ussgl480200_undelivered_or_cpe=ussgl480200_undelivered_or_cpe,
        ussgl483200_undelivered_or_cpe=ussgl483200_undelivered_or_cpe,
        ussgl488200_upward_adjustm_cpe=ussgl488200_upward_adjustm_cpe,
        gross_outlays_undelivered_fyb=gross_outlays_undelivered_fyb,
        gross_outlays_undelivered_cpe=gross_outlays_undelivered_cpe,
        ussgl490200_delivered_orde_cpe=ussgl490200_delivered_orde_cpe,
        ussgl490800_authority_outl_fyb=ussgl490800_authority_outl_fyb,
        ussgl490800_authority_outl_cpe=ussgl490800_authority_outl_cpe,
        ussgl498200_upward_adjustm_cpe=ussgl498200_upward_adjustm_cpe,
        gross_outlays_delivered_or_fyb=gross_outlays_delivered_or_fyb,
        gross_outlays_delivered_or_cpe=gross_outlays_delivered_or_cpe,
        gross_outlay_amount_by_pro_fyb=gross_outlay_amount_by_awa_fyb,
        gross_outlay_amount_by_pro_cpe=gross_outlay_amount_by_awa_cpe,
        obligations_incurred_by_pr_cpe=obligations_incurred_byawa_cpe,
        ussgl487100_downward_adjus_cpe=ussgl487100_downward_adjus_cpe,
        ussgl497100_downward_adjus_cpe=ussgl497100_downward_adjus_cpe,
        ussgl487200_downward_adjus_cpe=ussgl487200_downward_adjus_cpe,
        ussgl497200_downward_adjus_cpe=ussgl497200_downward_adjus_cpe,
        deobligations_recov_by_pro_cpe=deobligations_recov_by_awa_cpe,
        tas=tas,
        object_class=object_class,
        submission_id=submission_id
    )

    errors = number_of_errors(_FILE, database.stagingDb, models=[af1, af2, op])
    assert errors == 1