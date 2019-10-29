-- The combination of all the elements that make up the TAS must match the Treasury Central Accounting Reporting System
-- (CARS). AgencyIdentifier, MainAccountCode, and SubAccountCode are always required.
-- AllocationTransferAgencyIdentifier, BeginningPeriodOfAvailability, EndingPeriodOfAvailability and
-- AvailabilityTypeCode are required if present in the CARS table. tas_id will be null if the combination of these
-- elements is not in the system.
SELECT
    row_number,
    allocation_transfer_agency,
    agency_identifier,
    beginning_period_of_availa,
    ending_period_of_availabil,
    availability_type_code,
    main_account_code,
    sub_account_code,
    tas AS "uniqueid_TAS"
FROM appropriation AS approp
WHERE approp.submission_id = {0}
AND approp.tas_id IS NULL;
