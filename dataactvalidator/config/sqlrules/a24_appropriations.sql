-- StatusOfBudgetaryResourcesTotal_CPE = TotalBudgetaryResources_CPE
SELECT
    row_number,
    status_of_budgetary_resour_cpe,
    total_budgetary_resources_cpe,
    COALESCE(status_of_budgetary_resour_cpe, 0) - COALESCE(total_budgetary_resources_cpe, 0) AS "difference",
    tas AS "uniqueid_TAS"
FROM appropriation
WHERE submission_id = {0}
AND COALESCE(status_of_budgetary_resour_cpe, 0) <> COALESCE(total_budgetary_resources_cpe, 0);
