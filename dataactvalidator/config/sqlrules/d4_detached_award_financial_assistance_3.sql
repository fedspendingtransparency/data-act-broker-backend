-- A future ActionDate is valid only if it occurs within the current fiscal year
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
    action_date
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND pg_temp.is_date(COALESCE(action_date, '0'))
    AND CAST(action_date as DATE) > CURRENT_DATE
    AND EXTRACT(YEAR FROM (CURRENT_DATE + INTERVAL '3 month')) != EXTRACT(YEAR FROM CAST(action_date as DATE));