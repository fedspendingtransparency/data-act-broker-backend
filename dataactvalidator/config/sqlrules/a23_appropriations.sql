SELECT
    ap.row_number,
    ap.status_of_budgetary_resour_cpe,
    sf.amount
FROM appropriation as ap
    INNER JOIN sf_133 as sf ON (ap.tas = sf.tas)
WHERE submission_id = {} AND
    sf.line = 2500 AND
    COALESCE(ap.status_of_budgetary_resour_cpe, 0) <> sf.amount