-- Value of action date must be between 19991001 and 20991231 (i.e., a date between 10/01/1999 and 12/31/2099)
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
    AND
        ((CASE
            WHEN pg_temp.is_date(COALESCE(action_date, '0'))
            THEN
                CAST(action_date as DATE)
        END) < CAST('19991001' AS DATE)
        OR
        (CASE
            WHEN pg_temp.is_date(COALESCE(action_date, '0'))
            THEN
                CAST(action_date as DATE)
        END) > CAST('20991231' AS DATE));
