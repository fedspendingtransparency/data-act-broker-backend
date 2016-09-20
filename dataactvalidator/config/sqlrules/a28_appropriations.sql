SELECT
    approp.row_number,
    approp.other_budgetary_resources_cpe,
    approp.borrowing_authority_amount_cpe,
    approp.contract_authority_amount_cpe,
    approp.spending_authority_from_of_cpe
FROM appropriation AS approp
WHERE approp.submission_id = {}
    AND (approp.borrowing_authority_amount_cpe > 0
        OR approp.contract_authority_amount_cpe > 0
        OR approp.spending_authority_from_of_cpe > 0)
    AND (approp.other_budgetary_resources_cpe = 0 OR approp.other_budgetary_resources_cpe IS NULL);