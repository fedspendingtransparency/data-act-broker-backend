-- As of FY26, each row in file C must not contain a PAC/PAN.
SELECT
    row_number,
    program_activity_code,
    program_activity_name,
    display_tas AS "uniqueid_TAS",
    object_class AS "uniqueid_ObjectClass"
FROM award_financial AS af
    JOIN submission AS sub
        ON sub.submission_id = af.submission_id
WHERE af.submission_id = {0}
    AND reporting_fiscal_year >= 2026
    AND (COALESCE(program_activity_code, '') != ''
        OR COALESCE(program_activity_name, '') != ''
    );
