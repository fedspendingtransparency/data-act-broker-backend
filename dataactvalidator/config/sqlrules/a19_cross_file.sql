SELECT
    approp.row_number,
    approp.tas,
    approp.obligations_incurred_total_cpe,
    SUM(op.obligations_incurred_by_pr_cpe) as obligations_incurred_by_pr_cpe_sum
FROM appropriation approp
JOIN object_class_program_activity op
ON approp.tas = op.tas AND approp.submission_id = op.submission_id
WHERE approp.submission_id = {}
GROUP BY approp.row_number, approp.tas, approp.obligations_incurred_total_cpe
HAVING approp.obligations_incurred_total_cpe <> SUM(op.obligations_incurred_by_pr_cpe)
