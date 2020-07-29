-- Action date must follow YYYYMMDD format

SELECT
    row_number,
    action_date,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND CASE WHEN is_date(COALESCE(action_date, '0'))
        THEN action_date !~ '\d\d\d\d\d\d\d\d'
        ELSE TRUE
        END
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
