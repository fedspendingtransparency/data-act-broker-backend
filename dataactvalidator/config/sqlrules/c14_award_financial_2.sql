-- Rows must not contain a PIID and FAIN, or a PIID and URI.
SELECT row_number, fain, uri, piid
FROM award_financial
WHERE submission_id = {0}
    AND piid IS NOT NULL
    AND (fain IS NOT NULL
        OR uri IS NOT NULL
    );
