-- Unique FAIN or URI from file C should exist in file D2. Note that in File C:
-- 1) FAIN must be null for aggregate records;
-- and 2) URI must be null for non-aggregate records and PII-redacted non-aggregate records.
-- Do not process if allocation transfer agency is not null and does not match agency ID
-- (per C24, a non-SQL rule negation)
SELECT
    af.row_number AS "source_row_number",
    af.fain AS "source_value_fain",
    af.uri AS "source_value_uri",
    af.transaction_obligated_amou AS "source_value_transaction_obligated_amount",
    af.transaction_obligated_amou AS "difference",
    af.display_tas AS "uniqueid_TAS",
    af.fain AS "uniqueid_FAIN",
    af.uri AS "uniqueid_URI"
FROM award_financial AS af
WHERE af.submission_id = {0}
    AND af.transaction_obligated_amou IS NOT NULL
    AND af.fain IS NOT NULL
    AND (COALESCE(af.allocation_transfer_agency, '') = ''
        OR (COALESCE(af.allocation_transfer_agency, '') <> ''
            AND af.allocation_transfer_agency = af.agency_identifier
        )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM award_financial_assistance AS afa
        WHERE UPPER(af.fain) = UPPER(afa.fain)
            AND afa.record_type IN ('2', '3')
            AND afa.submission_id = {0}
    )
UNION ALL
SELECT
    af.row_number AS "source_row_number",
    af.fain AS "source_value_fain",
    af.uri AS "source_value_uri",
    af.transaction_obligated_amou AS "source_value_transaction_obligated_amount",
    af.transaction_obligated_amou AS "difference",
    af.display_tas AS "uniqueid_TAS",
    af.fain AS "uniqueid_FAIN",
    af.uri AS "uniqueid_URI"
FROM award_financial AS af
WHERE af.submission_id = {0}
    AND af.transaction_obligated_amou IS NOT NULL
    AND af.uri IS NOT NULL
    AND (COALESCE(af.allocation_transfer_agency, '') = ''
        OR (COALESCE(af.allocation_transfer_agency, '') <> ''
            AND af.allocation_transfer_agency = af.agency_identifier
        )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM award_financial_assistance AS afa
        WHERE UPPER(af.uri) = UPPER(afa.uri)
            AND afa.record_type = '1'
            AND afa.submission_id = {0}
    );
