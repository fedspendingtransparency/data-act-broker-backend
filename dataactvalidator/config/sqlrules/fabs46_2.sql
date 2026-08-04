-- IndirectCostFederalShareAmount is required for grants and cooperative agreements (AssistanceType = F001 or F002).
-- This only applies to award actions with ActionDate on or after April 4, 2022.
SELECT
    row_number,
    indirect_federal_sharing,
    assistance_type,
    action_date,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND indirect_federal_sharing IS NULL
    AND COALESCE(assistance_type, '') IN ('F001', 'F002')
    AND (CASE
            WHEN is_date(COALESCE(action_date, '0'))
            THEN CAST(action_date AS DATE)
        END) >= CAST('04/04/2022' AS DATE)
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
