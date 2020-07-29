-- Value of action date must be between 19991001 and 20991231 (i.e., a date between 10/01/1999 and 12/31/2099)

SELECT
    row_number,
    action_date,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND ((CASE WHEN is_date(COALESCE(action_date, '0'))
                THEN CAST(action_date AS DATE)
            END) < CAST('19991001' AS DATE)
        OR (CASE WHEN is_date(COALESCE(action_date, '0'))
                THEN CAST(action_date AS DATE)
            END) > CAST('20991231' AS DATE)
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
