CREATE OR REPLACE function pg_temp.is_zero(numeric) returns integer AS $$
BEGIN
    perform CAST($1 AS numeric);
    CASE WHEN $1 <> 0
        THEN return 1;
        ELSE return 0;
    END CASE;
EXCEPTION WHEN others THEN
    return 0;
END;
$$ LANGUAGE plpgsql;

SELECT
    op.row_number,
    op.object_class
FROM object_class_program_activity AS op
WHERE op.submission_id = {}
AND op.object_class IN ('0000', '000', '00', '0')
AND pg_temp.is_zero(op.deobligations_recov_by_pro_cpe) + pg_temp.is_zero(op.gross_outlay_amount_by_pro_cpe) +
    pg_temp.is_zero(op.gross_outlay_amount_by_pro_fyb) + pg_temp.is_zero(op.gross_outlays_delivered_or_cpe) +
    pg_temp.is_zero(op.gross_outlays_delivered_or_fyb) + pg_temp.is_zero(op.gross_outlays_undelivered_cpe) +
    pg_temp.is_zero(op.gross_outlays_undelivered_fyb) + pg_temp.is_zero(op.obligations_delivered_orde_cpe) +
    pg_temp.is_zero(op.obligations_delivered_orde_fyb) + pg_temp.is_zero(op.obligations_incurred_by_pr_cpe) +
    pg_temp.is_zero(op.obligations_undelivered_or_cpe) + pg_temp.is_zero(op.obligations_undelivered_or_fyb) +
    pg_temp.is_zero(op.ussgl480100_undelivered_or_cpe) + pg_temp.is_zero(op.ussgl480100_undelivered_or_fyb) +
    pg_temp.is_zero(op.ussgl480200_undelivered_or_cpe) + pg_temp.is_zero(op.ussgl480200_undelivered_or_fyb) +
    pg_temp.is_zero(op.ussgl483100_undelivered_or_cpe) + pg_temp.is_zero(op.ussgl483200_undelivered_or_cpe) +
    pg_temp.is_zero(op.ussgl487100_downward_adjus_cpe) + pg_temp.is_zero(op.ussgl487200_downward_adjus_cpe) +
    pg_temp.is_zero(op.ussgl488100_upward_adjustm_cpe) + pg_temp.is_zero(op.ussgl488200_upward_adjustm_cpe) +
    pg_temp.is_zero(op.ussgl490100_delivered_orde_cpe) + pg_temp.is_zero(op.ussgl490100_delivered_orde_fyb) +
    pg_temp.is_zero(op.ussgl490200_delivered_orde_cpe) + pg_temp.is_zero(op.ussgl490800_authority_outl_cpe) +
    pg_temp.is_zero(op.ussgl490800_authority_outl_fyb) + pg_temp.is_zero(op.ussgl493100_delivered_orde_cpe) +
    pg_temp.is_zero(op.ussgl497100_downward_adjus_cpe) + pg_temp.is_zero(op.ussgl497200_downward_adjus_cpe) +
    pg_temp.is_zero(op.ussgl498100_upward_adjustm_cpe) + pg_temp.is_zero(op.ussgl498200_upward_adjustm_cpe) <> 0;

