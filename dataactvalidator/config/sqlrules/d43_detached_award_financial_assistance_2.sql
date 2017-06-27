SELECT
    row_number,
    place_of_performance_congr,
    place_of_performance_zip4a
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(place_of_performance_zip4a, '') = ''
    AND COALESCE(place_of_performance_congr, '') = ''