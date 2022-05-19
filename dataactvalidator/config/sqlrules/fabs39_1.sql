-- PrimaryPlaceOfPerformanceCode is a required field for aggregate and non-aggregate records (RecordType = 1 or 2), and
-- must be in 00FORGN, 00*****, XX*****, XX**###, XX#####, or XX####R formats, where XX is a valid two-character state
-- code, # are numerals, and 'R' is that letter.
WITH fabs39_1_{0} AS
    (SELECT submission_id,
        row_number,
        record_type,
        place_of_performance_code,
        correction_delete_indicatr,
        afa_generated_unique
    FROM fabs
    WHERE submission_id = {0})
SELECT
    fabs.row_number,
    fabs.record_type,
    fabs.place_of_performance_code,
    fabs.afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs39_1_{0} AS fabs
WHERE fabs.record_type IN (1, 2)
    AND (COALESCE(fabs.place_of_performance_code, '') = ''
        OR (fabs.place_of_performance_code <> '00*****'
            AND UPPER(fabs.place_of_performance_code) <> '00FORGN'
            AND UPPER(fabs.place_of_performance_code) !~ '^[A-Z][A-Z]\*\*\*\*\*$'
            AND UPPER(fabs.place_of_performance_code) !~ '^[A-Z][A-Z]\*\*\d\d\d$'
            AND UPPER(fabs.place_of_performance_code) !~ '^[A-Z][A-Z]\d\d\d\d[\dR]$')
        OR (fabs.place_of_performance_code <> '00*****'
            AND UPPER(fabs.place_of_performance_code) <> '00FORGN'
            AND fabs.row_number NOT IN (
                SELECT DISTINCT sub_fabs.row_number
                FROM fabs39_1_{0} AS sub_fabs
                JOIN states
                    ON UPPER(SUBSTRING(sub_fabs.place_of_performance_code, 1, 2)) = states.state_code
            )
        )
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
