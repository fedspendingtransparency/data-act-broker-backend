-- AwardingOfficeCode must be six characters long.

SELECT
    row_number,
    awarding_office_code
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(awarding_office_code, '') != ''
    AND LENGTH(awarding_office_code) != 6