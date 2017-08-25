-- FederalActionObligation must be blank for loans (i.e., when AssistanceType = 07 or 08).
SELECT
    row_number,
    assistance_type,
    federal_action_obligation
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND (assistance_type = '07' OR assistance_type = '08')
    AND federal_action_obligation IS NOT NULL