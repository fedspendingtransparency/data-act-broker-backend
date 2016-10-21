SELECT row_number, fain, uri, piid
FROM award_financial
WHERE submission_id = {}
  AND piid IS NOT NULL
	AND (fain IS NOT NULL OR uri IS NOT NULL)