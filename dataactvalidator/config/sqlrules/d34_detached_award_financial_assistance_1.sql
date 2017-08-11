-- When both are provided, PeriodOfPerformanceStartDate must occur on or before PeriodOfPerformanceCurrentEndDate.
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
    period_of_performance_star,
    period_of_performance_curr
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND (CASE
            WHEN pg_temp.is_date(COALESCE(period_of_performance_star, '0'))
	        THEN CAST(period_of_performance_star as DATE)
	    END) >
	    (CASE
	        WHEN pg_temp.is_date(COALESCE(period_of_performance_curr, '0'))
		    THEN CAST(period_of_performance_curr as DATE)
		END)