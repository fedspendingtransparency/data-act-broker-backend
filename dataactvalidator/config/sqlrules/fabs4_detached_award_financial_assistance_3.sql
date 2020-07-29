-- A future ActionDate is valid only if it occurs within the current fiscal year

SELECT
    row_number,
    action_date,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND (CASE WHEN is_date(COALESCE(action_date, '0'))
            THEN CAST(action_date AS DATE)
        END) > CURRENT_DATE
    AND (CASE WHEN is_date(COALESCE(action_date, '0'))
            THEN EXTRACT(YEAR FROM CAST(action_date AS DATE) + INTERVAL '3 month')
        END) <> EXTRACT(YEAR FROM (CURRENT_DATE + INTERVAL '3 month'))
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
