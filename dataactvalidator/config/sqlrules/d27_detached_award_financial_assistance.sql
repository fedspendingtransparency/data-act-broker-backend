-- NonFederalFundingAmount must be blank for loans (i.e., when AssistanceType = 07 or 08).
SELECT
    row_number,
    assistance_type,
    non_federal_funding_amount
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND (assistance_type = '07' OR assistance_type = '08')
    AND non_federal_funding_amount IS NOT NULL