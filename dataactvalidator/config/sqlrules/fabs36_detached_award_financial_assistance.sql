-- CFDA_Number must be in XX.XXX format where # represents a number from 0 to 9.
SELECT
    row_number,
    cfda_number,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND dafa.cfda_number !~ '^\d\d\.\d\d\d$'
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
