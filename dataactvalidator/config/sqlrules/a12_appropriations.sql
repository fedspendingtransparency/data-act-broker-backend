SELECT
    approp.row_number,
    approp.adjustments_to_unobligated_cpe,
    SUM(sf.amount) as total_amount
FROM appropriation as approp
    INNER JOIN sf_133 as sf ON approp.tas = sf.tas
WHERE approp.submission_id = {} AND
    (sf.line >= 1010 AND sf.line <= 1042)
GROUP BY approp.row_number, approp.adjustments_to_unobligated_cpe
HAVING COALESCE(approp.adjustments_to_unobligated_cpe, 0) <> SUM(sf.amount)