-- OtherBudgetaryResourcesAmount_CPE = ContractAuthorityAmountTotal_CPE + BorrowingAuthorityAmountTotal_CPE +
-- SpendingAuthorityfromOffsettingCollectionsAmountTotal_CPE
SELECT
    row_number,
    other_budgetary_resources_cpe,
    contract_authority_amount_cpe,
    borrowing_authority_amount_cpe,
    spending_authority_from_of_cpe,
    other_budgetary_resources_cpe - (contract_authority_amount_cpe +
                                     borrowing_authority_amount_cpe +
                                     spending_authority_from_of_cpe) AS "variance"
FROM appropriation
WHERE submission_id = {0}
AND COALESCE(other_budgetary_resources_cpe, 0) <>
    COALESCE(contract_authority_amount_cpe, 0) +
    COALESCE(borrowing_authority_amount_cpe, 0) +
    COALESCE(spending_authority_from_of_cpe, 0);
