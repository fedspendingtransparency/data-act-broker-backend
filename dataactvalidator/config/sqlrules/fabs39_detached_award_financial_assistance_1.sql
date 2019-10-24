-- PrimaryPlaceOfPerformanceCode is a required field for aggregate and non-aggregate records (RecordType = 1 or 2), and
-- must be in 00FORGN, 00*****, XX*****, XX**###, XX#####, or XX####R formats, where XX is a valid two-character state
-- code, # are numerals, and 'R' is that letter.
WITH detached_award_financial_assistance_fabs39_1_{0} AS
    (SELECT submission_id,
        row_number,
        record_type,
        place_of_performance_code,
        correction_delete_indicatr,
        afa_generated_unique
    FROM detached_award_financial_assistance
    WHERE submission_id = {0})
SELECT
    dafa.row_number,
    dafa.record_type,
    dafa.place_of_performance_code,
    dafa.afa_generated_unique AS "uniqueid_afa_generated_unique"
FROM detached_award_financial_assistance_fabs39_1_{0} AS dafa
WHERE dafa.record_type IN (1, 2)
    AND (COALESCE(dafa.place_of_performance_code, '') = ''
        OR (dafa.place_of_performance_code <> '00*****'
            AND UPPER(dafa.place_of_performance_code) <> '00FORGN'
            AND UPPER(dafa.place_of_performance_code) !~ '^[A-Z][A-Z]\*\*\*\*\*$'
            AND UPPER(dafa.place_of_performance_code) !~ '^[A-Z][A-Z]\*\*\d\d\d$'
            AND UPPER(dafa.place_of_performance_code) !~ '^[A-Z][A-Z]\d\d\d\d[\dR]$')
        OR (dafa.place_of_performance_code <> '00*****'
            AND UPPER(dafa.place_of_performance_code) <> '00FORGN'
            AND dafa.row_number NOT IN (
                SELECT DISTINCT sub_dafa.row_number
                FROM detached_award_financial_assistance_fabs39_1_{0} AS sub_dafa
                JOIN states
                    ON UPPER(SUBSTRING(sub_dafa.place_of_performance_code, 1, 2)) = states.state_code
            )
        )
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
