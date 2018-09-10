-- When provided, FundingOfficeCode must be a valid value from the Federal Hierarchy.
SELECT
    row_number,
    funding_office_code
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND COALESCE(funding_office_code, '') <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM office
        WHERE office.office_code = dafa.funding_office_code
    );
