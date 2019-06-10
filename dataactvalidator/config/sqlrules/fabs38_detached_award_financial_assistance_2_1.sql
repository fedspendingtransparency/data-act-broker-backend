-- When provided, FundingOfficeCode must be a valid value from the Federal Hierarchy, including being designated
-- specifically as a Funding Office in the hierarchy.
SELECT
    row_number,
    funding_office_code
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND COALESCE(funding_office_code, '') <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM office
        WHERE UPPER(office.office_code) = UPPER(dafa.funding_office_code)
            AND (office.contract_funding_office = TRUE
                OR office.financial_assistance_funding_office = TRUE)
    );
