-- When provided, PeriodOfPerformanceStartDate must be a valid date between 19991001 and 20991231.
CREATE OR REPLACE function pg_temp.is_date(str text) returns boolean AS $$
BEGIN
  perform CAST(str AS DATE);
  return TRUE;
EXCEPTION WHEN others THEN
  return FALSE;
END;
$$ LANGUAGE plpgsql;

SELECT
    row_number,
    period_of_performance_star
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND
        ((CASE
            WHEN pg_temp.is_date(COALESCE(period_of_performance_star, '0'))
            THEN
                CAST(period_of_performance_star as DATE)
        END) < CAST('19991001' AS DATE)
        OR
        (CASE
            WHEN pg_temp.is_date(COALESCE(period_of_performance_star, '0'))
            THEN
                CAST(period_of_performance_star as DATE)
        END) > CAST('20991231' AS DATE));