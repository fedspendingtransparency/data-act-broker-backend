-- FundingOfficeCode must be six characters long.

SELECT
    row_number,
    funding_office_code
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND LENGTH(funding_office_code) != 6