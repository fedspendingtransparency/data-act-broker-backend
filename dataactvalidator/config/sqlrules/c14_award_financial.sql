SELECT row_number, fain, uri
FROM award_financial
WHERE submission_id = {}
	AND fain IS NULL
	AND uri IS NULL
	AND piid IS NULL;