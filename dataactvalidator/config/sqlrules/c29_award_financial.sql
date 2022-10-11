-- The File C GeneralLedgerPostDate must be blank for non-TOA (balance) rows.
SELECT
    row_number,
    general_ledger_post_date,
    display_tas AS "uniqueid_TAS",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    piid AS "uniqueid_PIID",
    fain AS "uniqueid_FAIN",
    uri AS "uniqueid_URI"
FROM award_financial
WHERE submission_id = {0}
    AND general_ledger_post_date IS NOT NULL
    AND transaction_obligated_amou IS NULL;
