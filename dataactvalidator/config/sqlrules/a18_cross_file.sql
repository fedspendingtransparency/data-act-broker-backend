SELECT
    approp.row_number,
    approp.tas,
    approp.gross_outlay_amount_by_tas_cpe,
    SUM(op.gross_outlay_amount_by_pro_cpe) as gross_outlay_amount_by_pro_cpe_sum
FROM appropriation approp JOIN object_class_program_activity op
ON approp.tas = op.tas AND approp.submission_id = op.submission_id
WHERE approp.submission_id = {}
GROUP BY approp.row_number, approp.tas, approp.gross_outlay_amount_by_tas_cpe
HAVING approp.gross_outlay_amount_by_tas_cpe <> SUM(op.gross_outlay_amount_by_pro_cpe)
