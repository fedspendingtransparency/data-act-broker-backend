-- When provided, FundingOfficeCode must be a valid value from the Federal Hierarchy,
-- including being designated specifically as an Assistance Funding Office in the hierarchy.
-- Since June 2019, the Federal Hierarchy has required that FundingOfficeCodes be flagged as either a
-- Procurement Funding Office or an Assistance Funding Office (or both).
-- The initial implementation of this distinction in FABS will allow either type to be acceptable.
SELECT
    row_number,
    funding_office_code,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(funding_office_code, '') <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM office
        WHERE UPPER(office.office_code) = UPPER(fabs.funding_office_code)
            AND (office.contract_funding_office = TRUE
                OR office.financial_assistance_funding_office = TRUE)
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
