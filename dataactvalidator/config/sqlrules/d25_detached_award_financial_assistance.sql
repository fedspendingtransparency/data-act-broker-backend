-- AwardDescription is required for non-aggregate records (i.e., when RecordType = 2).
SELECT
    row_number,
    record_type,
    award_description
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND record_type = 2
    AND (award_description IS NULL OR award_description = '')