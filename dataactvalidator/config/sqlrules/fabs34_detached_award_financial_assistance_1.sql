-- When both are provided, PeriodOfPerformanceStartDate must occur on or before PeriodOfPerformanceCurrentEndDate.

SELECT
    row_number,
    period_of_performance_star,
    period_of_performance_curr,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND (CASE WHEN is_date(COALESCE(period_of_performance_star, '0'))
                THEN CAST(period_of_performance_star AS DATE)
        END) >
        (CASE WHEN is_date(COALESCE(period_of_performance_curr, '0'))
            THEN CAST(period_of_performance_curr AS DATE)
        END)
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
