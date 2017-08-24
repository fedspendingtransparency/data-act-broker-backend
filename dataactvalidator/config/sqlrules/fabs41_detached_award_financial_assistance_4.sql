-- When provided, PrimaryPlaceofPerformanceZIP+4 must be in the format #####, #########, #####-####, or 'city-wide'
SELECT
    row_number,
    place_of_performance_zip4a
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(place_of_performance_zip4a, '') != ''
    AND place_of_performance_zip4a != 'city-wide'
    AND place_of_performance_zip4a !~ '^\d\d\d\d\d(\-?\d\d\d\d)?$'