-- FederalActionObligation must be blank for loans (i.e., when AssistanceType = 07 or 08).
SELECT
    row_number,
    assistance_type,
    federal_action_obligation,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND assistance_type IN ('07', '08')
    AND federal_action_obligation IS NOT NULL
    AND federal_action_obligation <> 0
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
