-- When PeriodOfPerformanceCurrentEndDate or PeriodOfPerformanceStartDate is provided, both fields should be provided.

SELECT
    row_number,
    period_of_performance_star,
    period_of_performance_curr,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND ((period_of_performance_star IS NOT NULL
            AND period_of_performance_curr IS NULL
        )
        OR (period_of_performance_star IS NULL
            AND period_of_performance_curr IS NOT NULL
        )
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
