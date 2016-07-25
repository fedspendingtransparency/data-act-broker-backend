SELECT
    ap.row_number,
    ap.obligations_incurred_total_cpe,
    sf.amount
FROM appropriation as ap
    INNER JOIN sf_133 as sf ON (ap.tas = sf.tas)
WHERE submission_id = {} AND
    sf.line = 2190 AND
    COALESCE(ap.obligations_incurred_total_cpe, 0) <> sf.amount