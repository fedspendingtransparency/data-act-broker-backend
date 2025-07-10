-- PeriodOfPerformanceCurrentEndDate, when provided, must follow YYYYMMDD format, where YYYY is the year, MM the month
-- and DD the day.

SELECT
    row_number,
    period_of_performance_curr,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(period_of_performance_curr, '') <> ''
    AND CASE WHEN is_date(COALESCE(period_of_performance_curr, '0'))
            THEN period_of_performance_curr !~ '\d\d\d\d\d\d\d\d'
            ELSE TRUE
        END
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
