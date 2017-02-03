-- AssistanceType field is required and must be one of the allowed values
SELECT
    dafa.row_number,
    dafa.assistance_type
FROM detached_award_financial_assistance as dafa
WHERE COALESCE(dafa.assistance_type, '') NOT IN ('02', '03', '04', '05', '06', '07', '08', '09', '10', '11')