-- The GrossOutlayAmountByTAS_CPE amount in the appropriations account file (A) must equal the sum of the
-- corresponding GrossOutlayAmountByProgramObjectClass_CPE values in the object class and program activity file (B)
-- where PYA = "X".
WITH appropriation_a18_{0} AS
    (SELECT row_number,
        gross_outlay_amount_by_tas_cpe,
        account_num,
        submission_id,
        display_tas
    FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number AS "source_row_number",
    UPPER(op.prior_year_adjustment) AS "target_prior_year_adjustment",
    approp.gross_outlay_amount_by_tas_cpe AS "source_value_gross_outlay_amount_by_tas_cpe",
    SUM(op.gross_outlay_amount_by_pro_cpe) AS "target_value_gross_outlay_amount_by_pro_cpe_sum",
    approp.gross_outlay_amount_by_tas_cpe - SUM(op.gross_outlay_amount_by_pro_cpe) AS "difference",
    approp.display_tas AS "uniqueid_TAS"
FROM appropriation_a18_{0} AS approp
    JOIN object_class_program_activity op
        ON approp.account_num = op.account_num
        AND approp.submission_id = op.submission_id
WHERE UPPER(op.prior_year_adjustment) = 'X'
GROUP BY approp.row_number,
    approp.gross_outlay_amount_by_tas_cpe,
    approp.display_tas,
    UPPER(op.prior_year_adjustment)
HAVING approp.gross_outlay_amount_by_tas_cpe <> SUM(op.gross_outlay_amount_by_pro_cpe);
