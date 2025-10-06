-- Rows must not contain a PIID and FAIN, or a PIID and URI.
SELECT
    row_number,
    fain,
    uri,
    piid,
    display_tas AS "uniqueid_TAS",
    piid AS "uniqueid_PIID",
    fain AS "uniqueid_FAIN",
    uri AS "uniqueid_URI"
FROM award_financial
WHERE submission_id = {0}
    AND (COALESCE(piid, '') <> ''
        AND (COALESCE(fain, '') <> ''
            OR COALESCE(uri, '') <> ''
        )
        OR (COALESCE(fain, '') <> ''
            AND COALESCE(uri, '') <> '')
    );
