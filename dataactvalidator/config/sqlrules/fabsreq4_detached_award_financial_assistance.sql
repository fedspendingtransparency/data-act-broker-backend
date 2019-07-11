-- BusinessFundsIndicator is required for all submissions except delete records, but was not provided in this row.
SELECT
    row_number,
    business_funds_indicator,
    correction_delete_indicatr
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(business_funds_indicator, '') = ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
