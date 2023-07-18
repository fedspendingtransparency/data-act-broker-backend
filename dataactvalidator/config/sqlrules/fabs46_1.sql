-- IndirectCostFederalShareAmount must be blank or 0 for AssistanceType 07, 08, and 09.
SELECT
    row_number,
    indirect_federal_sharing,
    assistance_type,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(indirect_federal_sharing, 0) <> 0
    AND COALESCE(assistance_type, '') IN ('07', '08', '09')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
