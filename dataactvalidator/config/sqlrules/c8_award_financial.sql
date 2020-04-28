-- Unique FAIN and/or URI from file C should exist in file D2. FAIN should be null for aggregate records. URI may be
-- null for non-aggregate records and PII-redacted non-aggregate records. Do not process if allocation transfer agency
-- is not null and does not match agency ID (per C24, a non-SQL rule negation)
WITH award_financial_c8_{0} AS
    (SELECT submission_id,
        row_number,
        allocation_transfer_agency,
        agency_identifier,
        transaction_obligated_amou,
        fain,
        uri,
        display_tas
    FROM award_financial
    WHERE submission_id = {0}),
award_financial_assistance_c8_{0} AS
    (SELECT submission_id,
        row_number,
        fain,
        uri,
        record_type
    FROM award_financial_assistance
    WHERE submission_id = {0})
SELECT
    af.row_number AS "source_row_number",
    af.fain AS "source_value_fain",
    af.uri AS "source_value_uri",
    af.display_tas AS "uniqueid_TAS",
    af.fain AS "uniqueid_FAIN",
    af.uri AS "uniqueid_URI"
FROM award_financial_c8_{0} AS af
WHERE af.transaction_obligated_amou IS NOT NULL
    AND (COALESCE(af.allocation_transfer_agency, '') = ''
        OR (COALESCE(af.allocation_transfer_agency, '') <> ''
            AND af.allocation_transfer_agency = af.agency_identifier
        )
    )
    AND ((af.fain IS NOT NULL
            AND NOT EXISTS (
                SELECT 1
                FROM award_financial_assistance_c8_{0} AS afa
                WHERE UPPER(af.fain) = UPPER(afa.fain)
                    AND afa.record_type IN ('2', '3')
            )
        )
        OR (af.uri IS NOT NULL
            AND NOT EXISTS (
                SELECT 1
                FROM award_financial_assistance_c8_{0} AS afa
                WHERE UPPER(af.uri) = UPPER(afa.uri)
                    AND afa.record_type = '1'
            )
        )
    );
