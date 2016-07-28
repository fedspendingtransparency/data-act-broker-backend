SELECT
    approp.row_number,
    approp.obligations_incurred_total_cpe,
    sf.amount
FROM appropriation as approp
    INNER JOIN sf_133 as sf ON (approp.tas = sf.tas)
WHERE submission_id = {} AND
    sf.line = 2190 AND
    COALESCE(approp.obligations_incurred_total_cpe, 0) <> sf.amount