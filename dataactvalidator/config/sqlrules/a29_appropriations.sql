SELECT
    approp.row_number,
    approp.deobligations_recoveries_r_cpe,
    SUM(sf.amount) as total_amount
FROM appropriation as approp
    INNER JOIN sf_133 as sf ON approp.tas = sf.tas
WHERE approp.submission_id = {} AND
    sf.line in (1021, 1033)
GROUP BY approp.row_number, approp.deobligations_recoveries_r_cpe
HAVING COALESCE(approp.deobligations_recoveries_r_cpe, 0) <> SUM(sf.amount)