# Zips Historical

## Background

Due to the census changes to congressional districts every decade, when these changes filter down to the Broker we store the most recent prior zip state to derive older FABS congressional districts. `zips_historical` is used to derive congressional districts and county codes within FABS for records where the `action_date` is earlier than the execution of the new census data. The following processes should be done every ten years.

## S3 Process

Copy the final prior version of the zip code data to a historical folder. This is done in December after the December USPS load for consistency. All files in the `zips/` folder must be copied without alterations to the `zips_historical/` folder in the relevant bucket.

## Database Process

On the same day as the files are moved, the contents of the `zips` table must be copied to the `zips_historical` table and the contents of the `zips_grouped` table must be copied to the `zips_grouped_historical` table using the following set of commands.

```
TRUNCATE zips_historical;

INSERT INTO zips_historical (created_at, updated_at, zip5, zip_last4, state_abbreviation, county_number, congressional_district_no)
SELECT created_at, updated_at, zip5, zip_last4, state_abbreviation, county_number, congressional_district_no
FROM zips;

TRUNCATE zips_grouped_historical;

INSERT INTO zips_grouped_historical (created_at, updated_at, zip5, state_abbreviation, county_number, congressional_district_no)
SELECT created_at, updated_at, zip5, state_abbreviation, county_number, congressional_district_no
FROM zips_grouped;
```

## Code Process
Within [fabs\_derivation\_helper.py](../dataactbroker/helpers/fabs_derivations_helper.py) update the `ZIP_DATE_CHANGE` to the new census turnover date.