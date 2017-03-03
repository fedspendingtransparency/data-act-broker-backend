-- BusinessFundsIndicator must contain one of the following values: REC or NON.
SELECT
    row_number,
    business_funds_indicator
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(business_funds_indicator) != 'REC'
    AND UPPER(business_funds_indicator) != 'NON'