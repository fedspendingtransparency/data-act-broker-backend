-- Must be a valid 3-digit object class as defined in OMB Circular A-11 Section 83.6, or a 4-digit code which includes
-- a 1-digit suffix that is typically zero. Do not include decimal points when reporting object classes
-- (e.g., "25.2" would be reported as 252 or 2520). For amounts that cannot yet be allocated to a valid object
-- class, input 000, although note that this will prompt a warning unless all obligation and outlay balances on this
-- row are $0. A fatal error will be given if Object Class is not provided.
SELECT
    row_number,
    object_class,
    display_tas AS "uniqueid_TAS",
    object_class AS "uniqueid_ObjectClass"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND object_class IN ('0000', '000', '00', '0')
    -- checking if any of the numeric values are non-zero
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
