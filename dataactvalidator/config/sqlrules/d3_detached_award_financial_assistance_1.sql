-- Action type is required for non-aggregate records (i.e., when RecordType = 2)
SELECT
    dafa.row_number,
    dafa.action_type,
    dafa.record_type
FROM detached_award_financial_assistance as dafa
WHERE dafa.record_type = 2
    AND COALESCE(dafa.action_type, '') = '' ;
