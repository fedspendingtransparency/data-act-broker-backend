-- Action date must follow YYYYMMDD format
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
    AND CASE WHEN pg_temp.is_date(COALESCE(action_date, '0'))
        THEN action_date !~ '\d\d\d\d\d\d\d\d'
        ELSE TRUE
        END