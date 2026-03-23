-- IndirectCostFederalShareAmount must be blank or 0 for AssistanceType 07, 08, 09, F003, F004, and F005.
SELECT
    row_number,
    indirect_federal_sharing,
    assistance_type,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(indirect_federal_sharing, 0) <> 0
    AND COALESCE(assistance_type, '') IN ('07', '08', '09', 'F003', 'F004', 'F005')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
