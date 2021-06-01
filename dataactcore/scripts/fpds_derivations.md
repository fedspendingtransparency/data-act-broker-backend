# FPDS Derivations

The following are explanations of the single and chained derivations used in the logic of [pull_fpds_data.py](./pull_fpds_data.py) 

## Single Derivations

These are derivations that can happen in any order.
* If `awarding_sub_tier_agency_c` is populated
	* `cgac.cgac_code` or `frec.frec_code` => `awarding_agency_code`
	* `cgac.cgac_name` or `frec.frec_name` => `awarding_agency_name`
* If `funding_sub_tier_agency_co` is populated
	* `cgac.cgac_code` or `frec.frec_code` => `funding_agency_code`
	* `cgac.cgac_name` or `frec.frec_name` => `funding_agency_name`
* Various boolean fields => `business_categories`
* If `awardee_or_recipient_uniqu` is populated and matches the DUNS data with executive compensation 
    * exec comp matching on `awardee_or_recipient_uniqu` => `high_comp_officerX_full_na`
	* exec comp matching on `awardee_or_recipient_uniqu` => `high_comp_officerX_amount`
* `piid`, `agency_id`, `parent_award_id`, `referenced_idv_agency_iden` or `piid`, `agency_id` => `unique_award_key`
* `agency_id`, `referenced_idv_agency_iden`, `piid`, `award_modification_amendme`, `parent_award_id`, `transaction_number` or `agency_id`, `piid`, `award_modification_amendme` => `detached_award_proc_unique`

## Chain Derivations

These are derivations that dependent on each other and must be executed in the right order.
1. If `place_of_perform_country_c` is populated, `calculate_ppop_fields`
	* If `place_of_perform_country_c` is populated
		* If `place_of_perform_country_c` != `USA`
			* country_code data matching on `place_of_perform_country_c` => `place_of_performance_state`
			* state_code data matching on `place_of_performance_state` => `place_of_perfor_state_desc`
			* `USA` => `place_of_perform_country_c`
			* `UNITED STATES` => `place_of_perf_country_desc`
		* If `place_of_perfor_state_desc` is still not populated
			* state_code data matching on `place_of_performance_state` => `place_of_perfor_state_desc`
			Note: this is for if `place_of_perform_country_c` = `USA`
		* If `place_of_perform_county_na` and `place_of_performance_state` are populated
			* county code matching our county and state data => `place_of_perform_county_co`
		* If `place_of_perform_county_co` isnt populated and `place_of_performance_zip4a` is
			* county code matching our `place_of_performance_zip4a` data => `place_of_perform_county_co`
		* If `place_of_perform_county_na` isnt populated
			* country name matching our `place_of_performance_state` and `place_of_perform_county_co` data => `place_of_perform_county_na`
		* If `place_of_performance_zip4a` is populated and in the normal zip format
			* first 5 numbers of `place_of_performance_zip4a` => `place_of_performance_zip5`
			* last 4 numbers of `place_of_performance_zip4a` => `place_of_perform_zip_last4` (if there are 9 numbers)
	* If `place_of_perf_country_desc` isnt populated
		* country data matching `place_of_perform_country_c` => `place_of_perf_country_desc`
2. If `legal_entity_country_code` is populated, `calculate_legal_entity_fields`
	* If `legal_entity_country_code` matches our country code data
		* If `legal_entity_country_code` != `USA`
			* country_code data matching on `legal_entity_country_code` => `legal_entity_state_code`
			* state_code data matching on `legal_entity_state_code` => `legal_entity_state_descrip`
			* `USA` => `legal_entity_country_code`
			* `UNITED STATES` => `legal_entity_country_name`
		* If `legal_entity_state_descrip` is still not populated
			* state_code data matching on `legal_entity_state_code` => `legal_entity_state_descrip`
			* Note: this is for if `legal_entity_country_code` = `USA`
		* If `place_of_performance_zip4a` is populated and in the normal zip format
			* county data matching the zip => `legal_entity_county_code`
			* county data matching the `legal_entity_county_code` and `legal_entity_state_code` => `legal_entity_county_name`
			* first 5 numbers of `place_of_performance_zip4a` => `place_of_performance_zip5`
			* last 4 numbers of `place_of_performance_zip4a` => `place_of_perform_zip_last4` (if there are 9 numbers)
	* If `legal_entity_country_name` isn't populated
		* country data matching `legal_entity_country_code` => `legal_entity_country_name`