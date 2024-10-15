-- Pre-FY26, each row in file C must contain either PAC/PAN or PARK.
SELECT
    row_number,
    pa_reporting_key,
    program_activity_code,
    program_activity_name,
    display_tas AS "uniqueid_TAS",
    object_class AS "uniqueid_ObjectClass"
FROM award_financial AS af
JOIN submission AS sub
    ON sub.submission_id = af.submission_id
WHERE af.submission_id = {0}
    AND reporting_fiscal_year < 2026
    AND COALESCE(pa_reporting_key, '') = ''
    AND (COALESCE(program_activity_code, '') = ''
        OR COALESCE(program_activity_name, '') = ''
    );