SELECT
    row_number,
    status_of_budgetary_resour_cpe,
    obligations_incurred_total_cpe,
    unobligated_balance_cpe
FROM appropriation
WHERE submission_id = {}
AND COALESCE(status_of_budgetary_resour_cpe,0) <> COALESCE(obligations_incurred_total_cpe,0) + COALESCE(unobligated_balance_cpe,0)

