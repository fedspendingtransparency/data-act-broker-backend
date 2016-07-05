SELECT
    row_number,
    status_of_budgetary_resour_cpe,
    budget_authority_available_cpe
FROM appropriation
WHERE submission_id = {}
AND COALESCE(status_of_budgetary_resour_cpe,0) <> COALESCE(budget_authority_available_cpe,0)

