-- Must be valid 3-digit object class as defined in OMB Circular A-11 Section 83.6, or a 4-digit code which includes a
-- 1-digit prefix that distinguishes direct, reimbursable, and allocation obligations. Do not include decimal points
-- when reporting in the Schema. Object Class Codes of 000 will prompt a warning.
CREATE OR REPLACE function pg_temp.is_zero(NUMERIC) returns INTEGER AS $$
BEGIN
    perform CAST($1 AS NUMERIC);
    CASE WHEN $1 <> 0
        THEN return 1;
        ELSE return 0;
    END CASE;
EXCEPTION WHEN others THEN
    return 0;
END;
$$ LANGUAGE plpgsql;

SELECT
    row_number,
    object_class,
    tas AS "uniqueid_TAS",
    object_class AS "uniqueid_ObjectClass"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND object_class IN ('0000', '000', '00', '0')
    -- checking if any of the numeric values are non-zero
    AND pg_temp.is_zero(deobligations_recov_by_pro_cpe) + pg_temp.is_zero(gross_outlay_amount_by_pro_cpe) +
        pg_temp.is_zero(gross_outlay_amount_by_pro_fyb) + pg_temp.is_zero(gross_outlays_delivered_or_cpe) +
        pg_temp.is_zero(gross_outlays_delivered_or_fyb) + pg_temp.is_zero(gross_outlays_undelivered_cpe) +
        pg_temp.is_zero(gross_outlays_undelivered_fyb) + pg_temp.is_zero(obligations_delivered_orde_cpe) +
        pg_temp.is_zero(obligations_delivered_orde_fyb) + pg_temp.is_zero(obligations_incurred_by_pr_cpe) +
        pg_temp.is_zero(obligations_undelivered_or_cpe) + pg_temp.is_zero(obligations_undelivered_or_fyb) +
        pg_temp.is_zero(ussgl480100_undelivered_or_cpe) + pg_temp.is_zero(ussgl480100_undelivered_or_fyb) +
        pg_temp.is_zero(ussgl480200_undelivered_or_cpe) + pg_temp.is_zero(ussgl480200_undelivered_or_fyb) +
        pg_temp.is_zero(ussgl483100_undelivered_or_cpe) + pg_temp.is_zero(ussgl483200_undelivered_or_cpe) +
        pg_temp.is_zero(ussgl487100_downward_adjus_cpe) + pg_temp.is_zero(ussgl487200_downward_adjus_cpe) +
        pg_temp.is_zero(ussgl488100_upward_adjustm_cpe) + pg_temp.is_zero(ussgl488200_upward_adjustm_cpe) +
        pg_temp.is_zero(ussgl490100_delivered_orde_cpe) + pg_temp.is_zero(ussgl490100_delivered_orde_fyb) +
        pg_temp.is_zero(ussgl490200_delivered_orde_cpe) + pg_temp.is_zero(ussgl490800_authority_outl_cpe) +
        pg_temp.is_zero(ussgl490800_authority_outl_fyb) + pg_temp.is_zero(ussgl493100_delivered_orde_cpe) +
        pg_temp.is_zero(ussgl497100_downward_adjus_cpe) + pg_temp.is_zero(ussgl497200_downward_adjus_cpe) +
        pg_temp.is_zero(ussgl498100_upward_adjustm_cpe) + pg_temp.is_zero(ussgl498200_upward_adjustm_cpe) <> 0;
