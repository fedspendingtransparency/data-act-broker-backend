SELECT
    ap.row_number,
    ap.deobligations_recoveries_r_cpe,
    SUM(sf.amount) as total_amount
FROM appropriation as ap
    INNER JOIN sf_133 as sf ON ap.tas = sf.tas
WHERE ap.submission_id = {} AND
    sf.line in (1021, 1033)
GROUP BY ap.row_number, ap.deobligations_recoveries_r_cpe
HAVING COALESCE(ap.deobligations_recoveries_r_cpe, 0) <> SUM(sf.amount)