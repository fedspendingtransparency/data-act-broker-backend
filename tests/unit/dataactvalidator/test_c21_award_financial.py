from random import randint

from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c21_award_financial'
_TAS = 'c21_award_financial_tas'

af_dict = dict(
        submission_id=randint(1000, 10000),
        tas='some-tas',
        program_activity_code='some-code',
        ussgl480100_undelivered_or_fyb=randint(-10000, -1000),
        ussgl480100_undelivered_or_cpe=randint(-10000, -1000),
        ussgl483100_undelivered_or_cpe=randint(-10000, -1000),
        ussgl488100_upward_adjustm_cpe=randint(-10000, -1000),
        obligations_undelivered_or_fyb=randint(-10000, -1000),
        obligations_undelivered_or_cpe=randint(-10000, -1000),
        ussgl490100_delivered_orde_fyb=randint(-10000, -1000),
        ussgl490100_delivered_orde_cpe=randint(-10000, -1000),
        ussgl493100_delivered_orde_cpe=randint(-10000, -1000),
        ussgl498100_upward_adjustm_cpe=randint(-10000, -1000),
        obligations_delivered_orde_fyb=randint(-10000, -1000),
        obligations_delivered_orde_cpe=randint(-10000, -1000),
        ussgl480200_undelivered_or_fyb=randint(-10000, -1000),
        ussgl480200_undelivered_or_cpe=randint(-10000, -1000),
        ussgl483200_undelivered_or_cpe=randint(-10000, -1000),
        ussgl488200_upward_adjustm_cpe=randint(-10000, -1000),
        gross_outlays_undelivered_fyb=randint(-10000, -1000),
        gross_outlays_undelivered_cpe=randint(-10000, -1000),
        ussgl490200_delivered_orde_cpe=randint(-10000, -1000),
        ussgl490800_authority_outl_fyb=randint(-10000, -1000),
        ussgl490800_authority_outl_cpe=randint(-10000, -1000),
        ussgl498200_upward_adjustm_cpe=randint(-10000, -1000),
        gross_outlays_delivered_or_fyb=randint(-10000, -1000),
        gross_outlays_delivered_or_cpe=randint(-10000, -1000),
        gross_outlay_amount_by_awa_fyb=randint(-10000, -1000),
        gross_outlay_amount_by_awa_cpe=randint(-10000, -1000),
        obligations_incurred_byawa_cpe=randint(-10000, -1000),
        ussgl487100_downward_adjus_cpe=randint(-10000, -1000),
        ussgl497100_downward_adjus_cpe=randint(-10000, -1000),
        ussgl487200_downward_adjus_cpe=randint(-10000, -1000),
        ussgl497200_downward_adjus_cpe=randint(-10000, -1000),
        deobligations_recov_by_awa_cpe=randint(-10000, -1000)
    )


def test_column_headers(database):
    expected_subset = {'row_number'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that the sum of financial elements in File C is less than or equal
     to the corresponding element in File B for the same TAS and Program Activity Code combination"""
    af1 = AwardFinancialFactory(**af_dict)
    af2 = AwardFinancialFactory(**af_dict)

    op = ObjectClassProgramActivityFactory(
        ussgl480100_undelivered_or_fyb=af_dict['ussgl480100_undelivered_or_fyb'] * 2,
        ussgl480100_undelivered_or_cpe=af_dict['ussgl480100_undelivered_or_cpe'] * 2,
        ussgl483100_undelivered_or_cpe=af_dict['ussgl483100_undelivered_or_cpe'] * 2,
        ussgl488100_upward_adjustm_cpe=af_dict['ussgl488100_upward_adjustm_cpe'] * 2,
        obligations_undelivered_or_fyb=af_dict['obligations_undelivered_or_fyb'] * 2,
        obligations_undelivered_or_cpe=af_dict['obligations_undelivered_or_cpe'] * 2,
        ussgl490100_delivered_orde_fyb=af_dict['ussgl490100_delivered_orde_fyb'] * 2,
        ussgl490100_delivered_orde_cpe=af_dict['ussgl490100_delivered_orde_cpe'] * 2,
        ussgl493100_delivered_orde_cpe=af_dict['ussgl493100_delivered_orde_cpe'] * 2,
        ussgl498100_upward_adjustm_cpe=af_dict['ussgl498100_upward_adjustm_cpe'] * 2,
        obligations_delivered_orde_fyb=af_dict['obligations_delivered_orde_fyb'] * 2,
        obligations_delivered_orde_cpe=af_dict['obligations_delivered_orde_cpe'] * 2,
        ussgl480200_undelivered_or_fyb=af_dict['ussgl480200_undelivered_or_fyb'] * 2,
        ussgl480200_undelivered_or_cpe=af_dict['ussgl480200_undelivered_or_cpe'] * 2,
        ussgl483200_undelivered_or_cpe=af_dict['ussgl483200_undelivered_or_cpe'] * 2,
        ussgl488200_upward_adjustm_cpe=af_dict['ussgl488200_upward_adjustm_cpe'] * 2,
        gross_outlays_undelivered_fyb=af_dict['gross_outlays_undelivered_fyb'] * 2,
        gross_outlays_undelivered_cpe=af_dict['gross_outlays_undelivered_cpe'] * 2,
        ussgl490200_delivered_orde_cpe=af_dict['ussgl490200_delivered_orde_cpe'] * 2,
        ussgl490800_authority_outl_fyb=af_dict['ussgl490800_authority_outl_fyb'] * 2,
        ussgl490800_authority_outl_cpe=af_dict['ussgl490800_authority_outl_cpe'] * 2,
        ussgl498200_upward_adjustm_cpe=af_dict['ussgl498200_upward_adjustm_cpe'] * 2,
        gross_outlays_delivered_or_fyb=af_dict['gross_outlays_delivered_or_fyb'] * 2,
        gross_outlays_delivered_or_cpe=af_dict['gross_outlays_delivered_or_cpe'] * 2,
        gross_outlay_amount_by_pro_fyb=af_dict['gross_outlay_amount_by_awa_fyb'] * 2,
        gross_outlay_amount_by_pro_cpe=af_dict['gross_outlay_amount_by_awa_cpe'] * 2,
        obligations_incurred_by_pr_cpe=af_dict['obligations_incurred_byawa_cpe'] * 2,
        ussgl487100_downward_adjus_cpe=af_dict['ussgl487100_downward_adjus_cpe'] * 2,
        ussgl497100_downward_adjus_cpe=af_dict['ussgl497100_downward_adjus_cpe'] * 2,
        ussgl487200_downward_adjus_cpe=af_dict['ussgl487200_downward_adjus_cpe'] * 2,
        ussgl497200_downward_adjus_cpe=af_dict['ussgl497200_downward_adjus_cpe'] * 2,
        deobligations_recov_by_pro_cpe=af_dict['deobligations_recov_by_awa_cpe'] * 2,
        tas=af_dict['tas'],
        program_activity_code=af_dict['program_activity_code'],
        submission_id=af_dict['submission_id']
    )

    errors = number_of_errors(_FILE, database, models=[af1, af2, op])
    assert errors == 0


def test_failure(database):
    """ Tests that the sum of financial elements in File C is not less than or equal
         to the corresponding element in File B for the same TAS and Program Activity Code combination"""
    af1 = AwardFinancialFactory(**af_dict)
    af2 = AwardFinancialFactory(**af_dict)

    op = ObjectClassProgramActivityFactory(
        ussgl480100_undelivered_or_fyb=af_dict['ussgl480100_undelivered_or_fyb'] * 2,
        ussgl480100_undelivered_or_cpe=af_dict['ussgl480100_undelivered_or_cpe'],
        ussgl483100_undelivered_or_cpe=af_dict['ussgl483100_undelivered_or_cpe'] * 2,
        ussgl488100_upward_adjustm_cpe=af_dict['ussgl488100_upward_adjustm_cpe'],
        obligations_undelivered_or_fyb=af_dict['obligations_undelivered_or_fyb'] * 2,
        obligations_undelivered_or_cpe=af_dict['obligations_undelivered_or_cpe'],
        ussgl490100_delivered_orde_fyb=af_dict['ussgl490100_delivered_orde_fyb'] * 2,
        ussgl490100_delivered_orde_cpe=af_dict['ussgl490100_delivered_orde_cpe'],
        ussgl493100_delivered_orde_cpe=af_dict['ussgl493100_delivered_orde_cpe'] * 2,
        ussgl498100_upward_adjustm_cpe=af_dict['ussgl498100_upward_adjustm_cpe'],
        obligations_delivered_orde_fyb=af_dict['obligations_delivered_orde_fyb'] * 2,
        obligations_delivered_orde_cpe=af_dict['obligations_delivered_orde_cpe'],
        ussgl480200_undelivered_or_fyb=af_dict['ussgl480200_undelivered_or_fyb'] * 2,
        ussgl480200_undelivered_or_cpe=af_dict['ussgl480200_undelivered_or_cpe'],
        ussgl483200_undelivered_or_cpe=af_dict['ussgl483200_undelivered_or_cpe'] * 2,
        ussgl488200_upward_adjustm_cpe=af_dict['ussgl488200_upward_adjustm_cpe'],
        gross_outlays_undelivered_fyb=af_dict['gross_outlays_undelivered_fyb'] * 2,
        gross_outlays_undelivered_cpe=af_dict['gross_outlays_undelivered_cpe'],
        ussgl490200_delivered_orde_cpe=af_dict['ussgl490200_delivered_orde_cpe'] * 2,
        ussgl490800_authority_outl_fyb=af_dict['ussgl490800_authority_outl_fyb'],
        ussgl490800_authority_outl_cpe=af_dict['ussgl490800_authority_outl_cpe'] * 2,
        ussgl498200_upward_adjustm_cpe=af_dict['ussgl498200_upward_adjustm_cpe'],
        gross_outlays_delivered_or_fyb=af_dict['gross_outlays_delivered_or_fyb'] * 2,
        gross_outlays_delivered_or_cpe=af_dict['gross_outlays_delivered_or_cpe'],
        gross_outlay_amount_by_pro_fyb=af_dict['gross_outlay_amount_by_awa_fyb'] * 2,
        gross_outlay_amount_by_pro_cpe=af_dict['gross_outlay_amount_by_awa_cpe'],
        obligations_incurred_by_pr_cpe=af_dict['obligations_incurred_byawa_cpe'] * 2,
        ussgl487100_downward_adjus_cpe=af_dict['ussgl487100_downward_adjus_cpe'],
        ussgl497100_downward_adjus_cpe=af_dict['ussgl497100_downward_adjus_cpe'] * 2,
        ussgl487200_downward_adjus_cpe=af_dict['ussgl487200_downward_adjus_cpe'],
        ussgl497200_downward_adjus_cpe=af_dict['ussgl497200_downward_adjus_cpe'] * 2,
        deobligations_recov_by_pro_cpe=af_dict['deobligations_recov_by_awa_cpe'],
        tas=af_dict['tas'],
        program_activity_code=af_dict['program_activity_code'],
        submission_id=af_dict['submission_id']
    )

    errors = number_of_errors(_FILE, database, models=[af1, af2, op])
    assert errors == 1