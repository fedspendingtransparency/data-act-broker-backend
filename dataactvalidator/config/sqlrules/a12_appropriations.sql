SELECT
    ap.row_number,
    ap.adjustments_to_unobligated_cpe,
    SUM(sf.amount) as total_amount
FROM appropriation as ap
    INNER JOIN sf_133 as sf ON ap.tas = sf.tas
WHERE ap.submission_id = {} AND
    (sf.line >= 1010 AND sf.line <= 1042)
GROUP BY ap.row_number, ap.adjustments_to_unobligated_cpe
HAVING COALESCE(ap.adjustments_to_unobligated_cpe, 0) <> SUM(sf.amount)