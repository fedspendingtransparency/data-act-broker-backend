SELECT row_number,
	beginning_period_of_availa,
	ending_period_of_availabil,
	agency_identifier,
	allocation_transfer_agency,
	availability_type_code,
	main_account_code,
	sub_account_code,
	object_class,
	program_activity_code,
	parent_award_id,
	piid,
	uri,
	fain,
	transaction_obligated_amou
FROM (
	SELECT af.row_number,
		af.beginning_period_of_availa,
		af.ending_period_of_availabil,
		af.agency_identifier,
		af.allocation_transfer_agency,
		af.availability_type_code,
		af.main_account_code,
		af.sub_account_code,
		af.object_class,
		af.program_activity_code,
		af.parent_award_id,
		af.piid,
		af.uri,
		af.fain,
		af.transaction_obligated_amou,
		af.submission_id,
		ROW_NUMBER() OVER (PARTITION BY
			af.beginning_period_of_availa,
			af.ending_period_of_availabil,
			af.agency_identifier,
			af.allocation_transfer_agency,
			af.availability_type_code,
			af.main_account_code,
			af.sub_account_code,
			af.object_class,
			af.program_activity_code,
			af.parent_award_id,
			af.piid,
			af.uri,
			af.fain,
			af.transaction_obligated_amou
		) AS row
	FROM award_financial as af
	WHERE af.submission_id = {0}
	ORDER BY af.row_number
	) duplicates
WHERE duplicates.row > 1;