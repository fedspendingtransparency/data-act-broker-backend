SELECT
    approp.row_number,
    approp.status_of_budgetary_resour_cpe,
    sf.amount
FROM appropriation as approp
    INNER JOIN sf_133 as sf ON (approp.tas = sf.tas)
WHERE submission_id = {} AND
    sf.line = 2500 AND
    COALESCE(approp.status_of_budgetary_resour_cpe, 0) <> sf.amount