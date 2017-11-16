-- Each row provided in file C (award financial) must contain either a FAIN, URI, or PIID.
SELECT row_number, fain, uri, piid
FROM award_financial
WHERE submission_id = {0}
    AND fain IS NULL
    AND uri IS NULL
    AND piid IS NULL;
