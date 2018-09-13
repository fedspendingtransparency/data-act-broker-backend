-- When provided, AwardingOfficeCode must be a valid value from the Federal Hierarchy.
SELECT
    row_number,
    awarding_office_code
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND COALESCE(awarding_office_code, '') <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM office
        WHERE office.office_code = dafa.awarding_office_code
    );
