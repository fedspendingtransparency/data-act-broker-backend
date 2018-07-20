-- Must be a valid program activity name and code for the corresponding TAS/TAFS as defined in Section 82 of OMB
-- Circular A-11. If the program activity is unknown, enter "0000" and "Unknown/Other" as your code and name,
-- respectively. The rule should not trigger at all for re-certifications of FY17Q2 and FY17Q3.
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

WITH object_class_program_activity_b9_{0} AS
    (SELECT *
    FROM object_class_program_activity
    WHERE submission_id = {0})
SELECT
    op.tas,
    op.submission_id,
    op.row_number,
    op.agency_identifier,
    op.main_account_code,
    op.program_activity_name,
    op.program_activity_code
FROM object_class_program_activity_b9_{0} AS op
     INNER JOIN submission AS sub
        ON op.submission_id = sub.submission_id
WHERE op.program_activity_code <> '0000'
    AND UPPER(op.program_activity_name) <> 'UNKNOWN/OTHER'
    AND NOT ((sub.reporting_fiscal_year, sub.reporting_fiscal_period) IN (('2017', 6), ('2017', 9))
        AND sub.publish_status_id <> 1)
    AND NOT EXISTS (
        SELECT 1
        FROM program_activity AS pa
            WHERE op.agency_identifier = pa.agency_id
            AND op.main_account_code = pa.account_number
            AND UPPER(COALESCE(op.program_activity_name, '')) = UPPER(pa.program_activity_name)
            AND COALESCE(op.program_activity_code, '') = pa.program_activity_code
            AND pa.fiscal_year_quarter = 'FY' || RIGHT(CAST(sub.reporting_fiscal_year AS CHAR(4)), 2) || 'Q' || sub.reporting_fiscal_period / 3
    )
    -- when there's no program activity name, return sum of true/false statements of whether all numerical values
    -- are zero or not (1 = not zero) (see if there are any non-zero values basically)
    AND (CASE WHEN op.program_activity_name = ''
            THEN pg_temp.is_zero(op.deobligations_recov_by_pro_cpe) + pg_temp.is_zero(op.gross_outlay_amount_by_pro_cpe) +
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
                pg_temp.is_zero(op.ussgl498100_upward_adjustm_cpe) + pg_temp.is_zero(op.ussgl498200_upward_adjustm_cpe)
            ELSE 1
        END) <> 0;
