-- AssistanceType field is required and must be one of the allowed values
SELECT
    row_number,
    assistance_type
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(assistance_type, '') NOT IN ('02', '03', '04', '05', '06', '07', '08', '09', '10', '11')