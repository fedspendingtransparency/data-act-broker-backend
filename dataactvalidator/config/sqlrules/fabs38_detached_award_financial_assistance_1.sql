-- FundingOfficeCode must be submitted for new awards (ActionType = A) or mixed aggregate records (ActionType = E)
-- whose ActionDate is on or after October 1, 2018, and whose CorrectionDeleteIndicator is either Blank or C.

SELECT
    row_number,
    funding_office_code,
    action_type,
    correction_delete_indicatr,
    action_date,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(funding_office_code, '') = ''
    AND UPPER(COALESCE(action_type, '')) IN ('A', 'E')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
    AND (CASE WHEN is_date(COALESCE(action_date, '0'))
            THEN CAST(action_date AS DATE)
        END) >= CAST('10/01/2018' AS DATE);
