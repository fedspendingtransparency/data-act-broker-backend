-- IndirectCostFederalShareAmount must be blank for non-grants/non-cooperative agreements
-- (AssistanceType = 06, 07, 08, 09, 10, or 11).
SELECT
    row_number,
    indirect_federal_sharing,
    assistance_type,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND indirect_federal_sharing IS NOT NULL
    AND COALESCE(assistance_type, '') IN ('06', '07', '08', '09', '10', '11')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
