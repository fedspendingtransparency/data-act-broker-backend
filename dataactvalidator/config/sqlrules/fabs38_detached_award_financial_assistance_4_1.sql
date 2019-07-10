-- When provided, AwardingOfficeCode must be a valid value from the Federal Hierarchy, including being designated
-- specifically as an Assistance/Grant Office in the hierarchy.
SELECT
    row_number,
    awarding_office_code
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND COALESCE(awarding_office_code, '') <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM office
        WHERE UPPER(office.office_code) = UPPER(dafa.awarding_office_code)
            AND office.financial_assistance_awards_office = TRUE
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
