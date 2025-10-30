-- Each row provided in file C (award financial) must contain either a FAIN, URI, or PIID.
SELECT
    row_number,
    fain,
    uri,
    piid,
    display_tas AS "uniqueid_TAS"
FROM award_financial
WHERE submission_id = {0}
    AND COALESCE(fain, '') = ''
    AND COALESCE(uri, '') = ''
    AND COALESCE(piid, '') = '';
