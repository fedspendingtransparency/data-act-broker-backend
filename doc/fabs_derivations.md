# FABS Derivations

The following are explanations of the single and chained derivations used in the logic of [fabs\_derivations\_helper.py](../dataactbroker/helpers/fabs_derivations_helper.py) 

## Single Derivations

These are derivations that can happen in any order.

**Note**: Any instance of `zips` that references congressional districts or county codes may use either `zips` or `zips_historical` depending on the `action_date`. See [zips_historical.md](zips_historical.md) for more details.

- `derive_total_funding_amount`
	- `federal_action_obligation` + `non_federal_funding_amount` => `total_funding_amount`
- `derive_cfda`
	- `cfda.program_title` => `cfda_title`
- `derive_awarding_agency_data`
	- `office.sub_tier_code` => `awarding_sub_tier_agency_c`
    - (based on `awarding_sub_tier_agency_c`)
		- `cgac.cgac_code` or `frec.frec_code` => `awarding_agency_code`
		- `cgac.cgac_name` or `frec.frec_name` => `awarding_agency_name`
		- `sta.sub_tier_agency_name` => `awarding_sub_tier_agency_n`
- `derive_funding_agency_data`
	- `office.sub_tier_code` => `funding_sub_tier_agency_c`
    - (based on `funding_sub_tier_agency_co`)
		- `cgac.cgac_code` or `frec.frec_code` => `funding_agency_code`
		- `cgac.cgac_name` or `frec.frec_name` => `funding_agency_name`
		- `sta.sub_tier_agency_name` => `funding_sub_tier_agency_n`
- `derive_office_data` (based on the award, the `awarding_sub_tier_agency_c`, and `record_type`)
	- `office.office_code` => `awarding_office_code`
	- `office.office_code` => `funding_office_code`
	- `office.office_name` => `awarding_office_name`
	- `office.office_name` => `funding_office_name`
- `derive_parent_uei` (based on `uei`)
	- `sam_recipient.ultimate_parent_legal_enti` => `ultimate_parent_legal_enti`
	- `sam_recipient.ultimate_parent_uei` => `ultimate_parent_uei`
- `derive_executive_compensation` (based on `uei`)
	- `sam_recipient.high_comp_officer1_full_na` => `high_comp_officer1_full_na`
	- `sam_recipient.high_comp_officer1_amount` => `high_comp_officer1_amount`
	- `sam_recipient.high_comp_officer2_full_na` => `high_comp_officer2_full_na`
	- `sam_recipient.high_comp_officer2_amount` => `high_comp_officer2_amount`
	- `sam_recipient.high_comp_officer3_full_na` => `high_comp_officer3_full_na`
	- `sam_recipient.high_comp_officer3_amount` => `high_comp_officer3_amount`
	- `sam_recipient.high_comp_officer4_full_na` => `high_comp_officer4_full_na`
	- `sam_recipient.high_comp_officer4_amount` => `high_comp_officer4_amount`
	- `sam_recipient.high_comp_officer5_full_na` => `high_comp_officer5_full_na`
	- `sam_recipient.high_comp_officer5_amount` => `high_comp_officer5_amount`
- `derive_labels`
	- `action_type_desc.description` => `action_type_description`
	- `assistance_type_description.description` => `assistance_type_desc`
	- `cdi_desc.description` => `correction_delete_ind_desc`
	- `record_type_desc.description` => `record_type_description`
	- `business_funds_ind_description.description` => `business_funds_ind_desc`
	- `business_type_desc.description` => `business_types_desc`
- `derive_fabs_business_categories`
	- `business_types` => `business_categories`
  

## Chain Derivations

These are derivations that dependent on each other and must be executed in the right order.

1. `derive_ppop_state` (start of chain)
	- If the first two characters of the `place_of_performance_code` match a state code from the states table
		- `states.state_code` => `place_of_perfor_state_code`
		- `states.state_name` => `place_of_perform_state_nam`
	- For records where `place_of_performance_code` = `00*****`,
		- `Multi-state` => `place_of_perform_state_nam`
2. `split_ppop_zip` (start of chain)
	- If `place_of_performance_zip4a` has the proper 5-9 format (`#####`,`#########`, `#####-####`)
		first 5 digits of `place_of_performance_zip4a` => `place_of_performance_zip5` 
		last 4 digits of `place_of_performance_zip4a` => `place_of_perform_zip_last4` (if the last 4 are provided)
3. derive_ppop_location_data (chains with #1 and #2)
	- If `place_of_perform_zip_last4` is populated and (`place_of_perform_zip_last4` and `place_of_performance_zip5` match our zips table)
		- `zips.congressional_district_no` => `place_of_performance_congr` (if not already populated) 
		- `zips.county_number` => `place_of_perform_county_co`
        - **Note: Derivation uses an older dataset if the action_date is before a certain date**
	- If `place_of_performance_zip5` matches our zip table and `place_of_performance_congr` is not populated
		-  `zips.congressional_district_no` => `place_of_performance_congr`
		- **Note: this will cover the instances where the zip5 was provided but not the zip4**
        - **Note: Derivation uses an older dataset if the action_date is before a certain date**
		- **Note: Derivation uses a threshold logic (75%) to associate the congressional district to a zip5 and state.**
	- If `place_of_performance_code` is in the county format, the county and state in it match our data, and `place_of_performance_congr` is not populated
		-  `cd_county_grouped.congressional_district_no` => `place_of_performance_congr`
		- **Note: Derivation uses a threshold logic (75%) to associate the congressional district to a county code and state.**
	- If `place_of_performance_code` is in the city format, the city and state in it match our data, and `place_of_performance_congr` is not populated
		-  `cd_city_grouped.congressional_district_no` => `place_of_performance_congr`
		- **Note: Derivation uses a threshold logic (75%) to associate the congressional district to a city code/name and state.**
	- If `place_of_performance_code` is in the state format, the state in it matches our data, and `place_of_performance_congr` is not populated
		-  `cd_state_grouped.congressional_district_no` => `place_of_performance_congr`
		- **Note: Derivation uses a threshold logic (100%) to associate the congressional district to a state.**
	- If `place_of_performance_zip5` matches our zip table and `place_of_perform_county_co` is not populated
		- `zips.county_number` => `place_of_perform_county_co`
		- **Note: this will cover the instances where the zip5 was provided but not the zip4**
        - **Note: Derivation uses an older dataset if the action_date is before a certain date**
	- If `place_of_performance_zip5` is populated and matches our zip city data,
		- `zip_city.city_name` => `place_of_performance_city`
	- If `place_of_performance_zip5` is not populated and `place_of_performance_code` has the 3 digits at the end (XX**###)
		- last 3 digits of `place_of_performance_code` => `place_of_perform_county_co`
	- If `place_of_performance_zip5` is not populated and `place_of_performance_code` has the 5 digits at the end (XX#####) and those digits along with the `place_of_perfor_state_code` match our city code data
		- `city_code.county_number` => `place_of_perform_county_co`
		- `city_code.county_name` => `place_of_perform_county_na`
		- `city_code.feature_name` => `place_of_performance_city`
	- If `place_of_perform_county_na` is not populated and `place_of_perform_county_co` is populated and `place_of_perform_county_co` along with `place_of_perfor_state_code` matches our county data
		- `county_code.county_name` => `place_of_perform_county_na`
	- Summary
		- `zips.congressional_district_no` => `place_of_performance_congr`
		- `zips.county_number` => `place_of_perform_county_co`
		- `zip_city.city_name` => `place_of_performance_city`
		- `place_of_performance_code` => `place_of_perform_county_co`
		- `city_code.county_number` => `place_of_perform_county_co`
		- `city_code.county_name` => `place_of_perform_county_na`
		- `city_code.feature_name` => `place_of_performance_city`
		- `county_code.county_name` => `place_of_perform_county_na`
4. derive_le_location_data (start of chain)
	- If `legal_entity_zip_last4` is populated, `legal_entity_zip_last4` and `legal_entity_zip5` match our zip data
		- `zips.congressional_district_no` => `legal_entity_congressional` (if not already populated)
		- `zips.county_number` => `legal_entity_county_code`
		- `zips.state_abbreviation` => `legal_entity_state_code`
        - **Note: Derivation uses an older dataset if the action_date is before a certain date**
	- If `legal_entity_zip5` matches our zip data and `legal_entity_state_code` isn't populated
		- `zip_city.state_code` => `legal_entity_state_code`
		- **Note: this will cover the instances where the zip5 was provided but not the zip4**
	- If `legal_entity_zip5` matches our zip data and `legal_entity_congressional` isn't populated
		- `zips.congressional_district_no` => `legal_entity_congressional`
		- **Note: this will cover the instances where the zip5 was provided but not the zip4**
        - **Note: Derivation uses an older dataset if the action_date is before a certain date**
		- **Note: Derivation uses a threshold logic (75%) to associate the congressional district to a zip5 and state.**
	- If `legal_entity_zip5` matches our zip data and `legal_entity_county_code` isn't populated
		- `zips.county_number` => `legal_entity_county_code`
		- **Note: this will cover the instances where the zip5 was provided but not the zip4**
        - **Note: Derivation uses an older dataset if the action_date is before a certain date**
	- If `legal_entity_zip5` is populated and `legal_entity_county_code` and `legal_entity_state_code` match our county data
		- `county_code.county_name` => `legal_entity_county_name`
	- If `legal_entity_zip5` is populated and `legal_entity_state_code` matches our state data
		- `states.state_name` => `legal_entity_state_name`
	- If `legal_entity_zip5` is populated and `legal_entity_zip5` matches our zip city data
		- `zip_city.city_name` => `legal_entity_city_name`
	- If `record_type` = `1` and `place_of_performance_code` matches the format (XX**###)
		- `place_of_perform_county_co` => `legal_entity_county_code`
		- `place_of_perform_county_na` => `legal_entity_county_name`
		- `place_of_perfor_state_code` => `legal_entity_state_code`
		- `place_of_perform_state_nam` => `legal_entity_state_name`
		- `place_of_performance_congr` => `legal_entity_congressional`
	- If `record_type` = `1` and `place_of_performance_code` matches the format (XX*****)
		- `place_of_perfor_state_code` => `legal_entity_state_code`
		- `place_of_perform_state_nam` => `legal_entity_state_name`
		- `place_of_performance_congr` => `legal_entity_congressional`
	Summary
		- `zips.congressional_district_no` => `legal_entity_congressional`
		- `zips.county_number` => `legal_entity_county_code`
		- `zips.state_abbreviation` => `legal_entity_state_code`
		- `county_code.county_name` => `legal_entity_county_name`
		- `states.state_name` => `legal_entity_state_name`
		- `zip_city.city_name` => `legal_entity_city_name`
		- If `record_type` = `1`
			- `place_of_perform_county_co` => `legal_entity_county_code`
			- `place_of_perform_county_na` => `legal_entity_county_name`
			- `place_of_perfor_state_code` => `legal_entity_state_code`
			- `place_of_perform_state_nam` => `legal_entity_state_name`
			- `place_of_performance_congr` => `legal_entity_congressional`
			- `place_of_perfor_state_code` => `legal_entity_state_code`
			- `place_of_perform_state_nam` => `legal_entity_state_name`
			- `place_of_performance_congr` => `legal_entity_congressional`
5. derive_le_city_code (chains with #4)
	- If `legal_entity_city_name` and `legal_entity_state_code` match with our city code data
		- `city_code.city_code` => `legal_entity_city_code`
6. derive_ppop_country_name (has to be before #8)
	- If `place_of_perform_country_c` matches with our country code data
		- `country_code.country_name` => `place_of_perform_country_n`
7. derive_le_country_name (chains with #8)
	- If `legal_entity_country_code` matches with our country code data
		- `country_code.country_name` => `legal_entity_country_name`
8. derive_pii_redacted_ppop_data (chains with #5, #7)
	- If `record_type` = `3` and `legal_entity_country_code` = `USA`
		- If legal_entity_state_code populated
			- `legal_entity_state_code` + (`legal_entity_city_code` or `0000`) => `place_of_performance_code`
		- `legal_entity_country_code` => `place_of_perform_country_c`
		- `legal_entity_country_name` => `place_of_perform_country_n`
		- `legal_entity_city_name` => `place_of_performance_city`
		- `legal_entity_county_code` => `place_of_perform_county_co`
		- `legal_entity_county_name` => `place_of_perform_county_na`
		- `legal_entity_state_code` => `place_of_perfor_state_code`
		- `legal_entity_state_name` => `place_of_perform_state_nam`
		- `legal_entity_zip5` => `place_of_performance_zip4a`
		- `legal_entity_zip5` => `place_of_performance_zip5`
		- `legal_entity_congressional` => `place_of_performance_congr`
	- If `record_type` = `3` and `legal_entity_country_code` != `USA`
		- `00FORGN` => `place_of_performance_code`
		- `legal_entity_country_code` => `place_of_perform_country_c`
		- `legal_entity_country_name` => `place_of_perform_country_n`
		- `legal_entity_foreign_city` => `place_of_performance_city`
		- `legal_entity_foreign_city` => `place_of_performance_forei`
9. derive_ppop_scope (possibly chains with #8)
	- If `place_of_performance_zip4a` is populated and matches the format (XX##### or XX####R)
		- If `place_of_performance_zip4a` = `CITY-WIDE`
			- `City-wide` => `place_of_performance_scope`
		- If `place_of_performance_zip4a` has the proper 5-9 format (`#####`,`#########`#####-####`)
			- `Single ZIP Code` => `place_of_performance_scope`
	- If `place_of_performance_zip4a` is not populated
		- If `place_of_performance_code` matches the format (XX##### or XX####R)
			- `City-wide` => `place_of_performance_scope`
		- If `place_of_performance_code` matches the format (XX**###)
			- `County-wide` => `place_of_performance_scope`
		- If `place_of_performance_code` matches the format (XX*****)
			- `State-wide` => `place_of_performance_scope`
		- If `place_of_performance_code` matches the format (00*****)
			- `Multi-state` => `place_of_performance_scope`
		- If `place_of_performance_code` = `00FORGN`
			- `Foreign` => `place_of_performance_scope`