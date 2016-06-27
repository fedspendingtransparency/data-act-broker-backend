SELECT approp.row_number, approp.tas, approp.obligationsincurredtotalbytas_cpe, SUM(op.obligationsincurredbyprogramobjectclass_cpe) as obligationsincurredbyprogramobjectclass_cpe_sum
FROM appropriation approp
    JOIN object_class_program_activity op ON approp.tas = op.tas AND approp.submission_id = op.submission_id
WHERE approp.submission_id = {}
GROUP BY approp.row_number, approp.tas, approp.obligationsincurredtotalbytas_cpe HAVING approp.obligationsincurredtotalbytas_cpe <> SUM(op.obligationsincurredbyprogramobjectclass_cpe)