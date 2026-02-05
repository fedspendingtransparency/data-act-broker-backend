-- PeriodOfPerformanceStartDate and PeriodOfPerformanceCurrentEndDate are required for Grants and Cooperative
-- Agreements (AssistanceType = 02, 03, 04, 05, F001, and F002).

SELECT
    row_number,
    period_of_performance_star,
    period_of_performance_curr,
    assistance_type,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(assistance_type, '') IN ('02', '03', '04', '05', 'F001', 'F002')
    AND (COALESCE(period_of_performance_star, '') = ''
            OR COALESCE(period_of_performance_curr, '') = ''
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
