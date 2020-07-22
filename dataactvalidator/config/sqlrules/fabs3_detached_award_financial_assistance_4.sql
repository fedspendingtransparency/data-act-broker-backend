-- ActionType = E is only valid for mixed aggregate records (RecordType = 1.)
SELECT
    row_number,
    action_type,
    record_type,
    correction_delete_indicatr,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(UPPER(correction_delete_indicatr), '') <> 'D'
    AND COALESCE(UPPER(action_type), '') = 'E'
    AND record_type <> 1;
