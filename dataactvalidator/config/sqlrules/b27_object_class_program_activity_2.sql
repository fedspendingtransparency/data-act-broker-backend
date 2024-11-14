-- Pre-FY26, each row in file B must contain either PAC/PAN or PARK.
SELECT
    row_number,
    pa_reporting_key,
    program_activity_code,
    program_activity_name,
    display_tas AS "uniqueid_TAS",
    object_class AS "uniqueid_ObjectClass"
FROM object_class_program_activity AS ocpa
JOIN submission AS sub
    ON sub.submission_id = ocpa.submission_id
WHERE ocpa.submission_id = {0}
    AND reporting_fiscal_year < 2026
    AND COALESCE(pa_reporting_key, '') = ''
    AND (COALESCE(program_activity_code, '') = ''
        OR COALESCE(program_activity_name, '') = ''
    );