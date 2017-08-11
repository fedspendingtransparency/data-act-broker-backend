-- While they are optional fields, if either PeriodOfPerformanceCurrentEndDate or PeriodOfPerformanceStartDate is
-- provided, both fields must be provided.
SELECT
    row_number,
    period_of_performance_star,
    period_of_performance_curr
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND ((period_of_performance_star IS NOT NULL AND period_of_performance_curr IS NULL)
        OR
        (period_of_performance_star IS NULL AND period_of_performance_curr IS NOT NULL))