-- AssistanceType field is required and must be one of the allowed values
SELECT
    row_number,
    assistance_type,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(assistance_type, '') NOT IN ('F001', 'F002', 'F003', 'F004', 'F005', 'F006', 'F007', 'F008', 'F009',
                                              'F010')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
