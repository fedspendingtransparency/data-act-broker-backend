-- BusinessFundsIndicator must contain one of the following values: REC or NON.
SELECT
    row_number,
    business_funds_indicator,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND UPPER(business_funds_indicator) NOT IN ('REC', 'NON')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
