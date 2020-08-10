-- File B (object class program activity): Reimbursable flag indicator is not required if object class is 4 digits. But
-- if either "D" or "R" are given, then they have to correspond to the first digit of object class,
-- R for 1XXX and D for 2XXX.
SELECT
    row_number,
    object_class,
    by_direct_reimbursable_fun,
    display_tas AS "uniqueid_TAS",
    object_class AS "uniqueid_ObjectClass"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND LENGTH(object_class) = 4
    AND NOT COALESCE(by_direct_reimbursable_fun, '') = ''
    AND NOT (SUBSTRING(object_class, 1, 1) = '1'
                AND UPPER(by_direct_reimbursable_fun) = 'D'
    )
    AND NOT (SUBSTRING(object_class, 1, 1) = '2'
                AND UPPER(by_direct_reimbursable_fun) = 'R'
    )
    AND is_zero(deobligations_recov_by_pro_cpe) + is_zero(gross_outlay_amount_by_pro_cpe) +
        is_zero(gross_outlay_amount_by_pro_fyb) + is_zero(gross_outlays_delivered_or_cpe) +
        is_zero(gross_outlays_delivered_or_fyb) + is_zero(gross_outlays_undelivered_cpe) +
        is_zero(gross_outlays_undelivered_fyb) + is_zero(obligations_delivered_orde_cpe) +
        is_zero(obligations_delivered_orde_fyb) + is_zero(obligations_incurred_by_pr_cpe) +
        is_zero(obligations_undelivered_or_cpe) + is_zero(obligations_undelivered_or_fyb) +
        is_zero(ussgl480100_undelivered_or_cpe) + is_zero(ussgl480100_undelivered_or_fyb) +
        is_zero(ussgl480200_undelivered_or_cpe) + is_zero(ussgl480200_undelivered_or_fyb) +
        is_zero(ussgl483100_undelivered_or_cpe) + is_zero(ussgl483200_undelivered_or_cpe) +
        is_zero(ussgl487100_downward_adjus_cpe) + is_zero(ussgl487200_downward_adjus_cpe) +
        is_zero(ussgl488100_upward_adjustm_cpe) + is_zero(ussgl488200_upward_adjustm_cpe) +
        is_zero(ussgl490100_delivered_orde_cpe) + is_zero(ussgl490100_delivered_orde_fyb) +
        is_zero(ussgl490200_delivered_orde_cpe) + is_zero(ussgl490800_authority_outl_cpe) +
        is_zero(ussgl490800_authority_outl_fyb) + is_zero(ussgl493100_delivered_orde_cpe) +
        is_zero(ussgl497100_downward_adjus_cpe) + is_zero(ussgl497200_downward_adjus_cpe) +
        is_zero(ussgl498100_upward_adjustm_cpe) + is_zero(ussgl498200_upward_adjustm_cpe) <> 0;
