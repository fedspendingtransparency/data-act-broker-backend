# Historical Location

## Zips Historical

### Background

Due to the census changes to congressional districts every decade, when these changes filter down to the Broker we store the most recent prior zip state to derive older FABS congressional districts. `zips_historical` is used to derive congressional districts and county codes within FABS for records where the `action_date` is earlier than the execution of the new census data. The following processes should be done every ten years.

### S3 Process

Copy the final prior version of the zip code data to a historical folder. This is done in December after the December USPS load for consistency. All files in the `zips/` folder must be copied without alterations to the `zips_historical/` folder in the relevant bucket.

### Database Process

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

### Code Process
Within [fabs\_derivation\_helper.py](../dataactbroker/helpers/fabs_derivations_helper.py) update the `ZIP_DATE_CHANGE` to the new census turnover date.

## Census

The file `census_congressional_districts.csv` contains data for changes in the congressional districts for each state through the census. It contains both the congressional districts that were removed in each census and the newly-added congressional districts from the latest census. All districts from this file will end up in the `state_congressional` table, with districts with `NULL` census years indicating that they were present in the previous census and also in the current one, in a sense making them both historical and current.

### Removed
Districts labeled with `removed` in the file are used to confirm historical districts in the validation rules. These districts may, at some point, be re-added to the list but will remain in the file for tracking purposes. In these situations, `census_year` means the last census year they were present as valid congressional districts. For example, if the year is `2010` that means the district existed in 2010 but was removed in the 2020 census. When running validations for historical values, both these and the `NULL` census year districts should be used.

### Added
Districts labeled with `added` in the file are used to differentiate between the "consistent" districts and the new ones, as simply leaving new districts with `NULL` census years would result in them also being used for historical validations. These are removed each new census and replaced with the new batch that have been added, meaning only one year of `added` should ever be present and it should never overlap with any `removed` year. When running validations for current values, both these and the `NUll` census year districts should be used.