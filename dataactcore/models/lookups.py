# This file defines a series of constants that represent the values used in the
# broker's "helper" tables. Rather than define the values in the db setup scripts
# and then make db calls to lookup the surrogate keys, we'll define everything
# here, in a file that can be used by the db setup scripts *and* the application
# code.

from collections import namedtuple

from dataactcore.models.stagingModels import (
    AwardFinancialAssistance, AwardFinancial, Appropriation, ObjectClassProgramActivity, AwardProcurement,
    DetachedAwardFinancialAssistance)

LookupType = namedtuple('LookupType', ['id', 'name', 'desc'])
LookupFileType = namedtuple('LookupFileType', ['id', 'name', 'desc', 'letter', 'order', 'crossfile', 'model'])

FILE_STATUS = [
    LookupType(1, 'complete', 'File has been processed'),
    LookupType(2, 'header_error', 'The file has errors in the header row'),
    LookupType(3, 'unknown_error', 'An unknown error has occurred with this file'),
    LookupType(4, 'single_row_error', 'Error occurred in job manager'),
    LookupType(5, 'job_error', 'File has not yet been validated'),
    LookupType(6, 'incomplete', 'File has not yet been validated'),
    LookupType(7, 'encoding_error', 'File contains invalid characters that could not be validated'),
    LookupType(8, 'row_count_error', 'Raw file row count does not match the number of rows validated'),
    LookupType(9, 'file_type_error', 'Invalid file type. Valid file types include .csv and .txt')
]
FILE_STATUS_DICT = {item.name: item.id for item in FILE_STATUS}
FILE_STATUS_DICT_ID = {item.id: item.name for item in FILE_STATUS}

ERROR_TYPE = [
    LookupType(1, 'type_error', 'The value provided was of the wrong type. Note that all type errors in a line'
                                ' must be fixed before the rest of the validation logic is applied to that line.'),
    LookupType(2, 'required_error', 'This field is required for all submissions but was not provided in this row.'),
    LookupType(3, 'value_error', 'The value provided was invalid.'),
    LookupType(4, 'read_error', 'Could not parse this record correctly.'),
    LookupType(5, 'write_error', 'Could not write this record into the staging table.'),
    LookupType(6, 'rule_failed', 'A rule failed for this value.'),
    LookupType(7, 'length_error', 'Value was longer than allowed length.')
]
ERROR_TYPE_DICT = {item.name: item.id for item in ERROR_TYPE}

JOB_STATUS = [
    LookupType(1, 'waiting', 'check dependency table'),
    LookupType(2, 'ready', 'can be assigned'),
    LookupType(3, 'running', 'job is currently in progress'),
    LookupType(4, 'finished', 'job is complete'),
    LookupType(5, 'invalid', 'job is invalid'),
    LookupType(6, 'failed', 'job failed to complete')
]
JOB_STATUS_DICT = {item.name: item.id for item in JOB_STATUS}
JOB_STATUS_DICT_ID = {item.id: item.name for item in JOB_STATUS}

JOB_TYPE = [
    LookupType(1, 'file_upload', 'file must be uploaded to S3'),
    LookupType(2, 'csv_record_validation', 'do record level validation and add to staging table'),
    LookupType(4, 'validation', 'new information must be validated')
]
JOB_TYPE_DICT = {item.name: item.id for item in JOB_TYPE}
JOB_TYPE_DICT_ID = {item.id: item.name for item in JOB_TYPE}

PUBLISH_STATUS = [
    LookupType(1, 'unpublished', 'Has not yet been moved to data store'),
    LookupType(2, 'published', 'Has been moved to data store'),
    LookupType(3, 'updated', 'Submission was updated after being published'),
    LookupType(4, 'publishing', 'Submission is being published')
]
PUBLISH_STATUS_DICT = {item.name: item.id for item in PUBLISH_STATUS}
PUBLISH_STATUS_DICT_ID = {item.id: item.name for item in PUBLISH_STATUS}

FILE_TYPE = [
    LookupFileType(1, 'appropriations', '', 'A', 1, True, Appropriation),
    LookupFileType(2, 'program_activity', '', 'B', 2, True, ObjectClassProgramActivity),
    LookupFileType(3, 'award_financial', '', 'C', 3, True, AwardFinancial),
    LookupFileType(4, 'award', '', 'D2', 4, True, AwardFinancialAssistance),
    LookupFileType(5, 'award_procurement', '', 'D1', 5, True, AwardProcurement),
    LookupFileType(6, 'executive_compensation', '', 'E', None, False, None),
    LookupFileType(7, 'sub_award', '', 'F', None, False, None),
    LookupFileType(8, 'fabs', '', 'FABS', None, False, DetachedAwardFinancialAssistance)
]
FILE_TYPE_DICT = {item.name: item.id for item in FILE_TYPE}
FILE_TYPE_DICT_ID = {item.id: item.name for item in FILE_TYPE}
FILE_TYPE_DICT_LETTER = {item.id: item.letter for item in FILE_TYPE}
FILE_TYPE_DICT_LETTER_ID = {item.letter: item.id for item in FILE_TYPE}
FILE_TYPE_DICT_LETTER_NAME = {item.letter: item.name for item in FILE_TYPE}

PERMISSION_TYPES = [
    LookupType(1, 'reader', 'This user is allowed to view any submission for their agency'),
    LookupType(2, 'writer', 'This user is allowed to create and edit any submission for their agency'),
    LookupType(3, 'submitter', 'This user is allowed to certify and submit any submission for their agency'),
    # Placeholder 4: website_admin
    LookupType(5, 'fabs', 'This user is allowed to create and publish any FABS data for their agency')
]
PERMISSION_TYPE_DICT = {item.name: item.id for item in PERMISSION_TYPES[:3]}
PERMISSION_TYPE_DICT_ID = {item.id: item.name for item in PERMISSION_TYPES}
PERMISSION_SHORT_DICT = {item.name[0]: item.id for item in PERMISSION_TYPES}

FIELD_TYPE = [
    LookupType(1, 'INT', 'integer type'),
    LookupType(2, 'DECIMAL', 'decimal type '),
    LookupType(3, 'BOOLEAN', 'yes/no'),
    LookupType(4, 'STRING', 'string type'),
    LookupType(5, 'LONG', 'long integer')
]
FIELD_TYPE_DICT = {item.name: item.id for item in FIELD_TYPE}
FIELD_TYPE_DICT_ID = {item.id: item.name for item in FIELD_TYPE}

RULE_SEVERITY = [
    LookupType(1, 'warning', 'warning'),
    LookupType(2, 'fatal', 'fatal error')
]
RULE_SEVERITY_DICT = {item.name: item.id for item in RULE_SEVERITY}

SUBMISSION_TYPE = [
    LookupType(1, 'all', 'Warning for all pages'),
    LookupType(2, 'dabs', 'Warning for DABS pages'),
    LookupType(3, 'fabs', 'Warning for FABS pages')
]
SUBMISSION_TYPE_DICT = {item.name: item.id for item in SUBMISSION_TYPE}

ACTION_TYPE = [
    LookupType(1, 'A', 'New'),
    LookupType(2, 'B', 'Continuation'),
    LookupType(3, 'C', 'Revision'),
    LookupType(4, 'D', 'Adjustment to Completed Project')
]
ACTION_TYPE_DICT = {item.name: item.desc for item in ACTION_TYPE}

ASSISTANCE_TYPE = [
    LookupType(1, '02', 'block grant (A)'),
    LookupType(2, '03', 'formula grant (A)'),
    LookupType(3, '04', 'project grant (B)'),
    LookupType(4, '05', 'cooperative agreement (B)'),
    LookupType(5, '06', 'direct payment for specified use, as a subsidy or other non-reimbursable direct financial aid '
                        '(C)'),
    LookupType(6, '07', 'direct loan (E)'),
    LookupType(7, '08', 'guaranteed/insured loan (F)'),
    LookupType(8, '09', 'insurance (G)'),
    LookupType(9, '10', 'direct payment with unrestricted use (retirement, pension, veterans benefits, etc.) (D)'),
    LookupType(10, '11', 'other reimbursable, contingent, intangible, or indirect financial assistance'),
]
ASSISTANCE_TYPE_DICT = {item.name: item.desc for item in ASSISTANCE_TYPE}

CORRECTION_DELETE_IND = [
    LookupType(1, 'C', 'Correct an Existing Record'),
    LookupType(2, 'D', 'Delete an Existing Record')
]
CORRECTION_DELETE_IND_DICT = {item.name: item.desc for item in CORRECTION_DELETE_IND}

RECORD_TYPE = [
    LookupType(1, 1, 'Aggregate Record'),
    LookupType(2, 2, 'Non-Aggregate Record'),
    LookupType(3, 3, 'Non-Aggregate Record to an Individual Recipient (PII-Redacted)')
]
RECORD_TYPE_DICT = {item.name: item.desc for item in RECORD_TYPE}

BUSINESS_TYPE = [
    LookupType(1, 'A', 'State Government'),
    LookupType(2, 'B', 'County Government'),
    LookupType(3, 'C', 'City or Township Government'),
    LookupType(4, 'D', 'Special District Government'),
    LookupType(5, 'E', 'Regional Organization'),
    LookupType(6, 'F', 'U.S. Territory or Possession'),
    LookupType(7, 'G', 'Independent School District'),
    LookupType(8, 'H', 'Public/State Controlled Institution of Higher Education'),
    LookupType(9, 'I', 'Indian/Native American Tribal Government (Federally-Recognized)'),
    LookupType(10, 'J', 'Indian/Native American Tribal Government (Other than Federally-Recognized)'),
    LookupType(11, 'K', 'Indian/Native American Tribal Designated Organization'),
    LookupType(12, 'L', 'Public/Indian Housing Authority'),
    LookupType(13, 'M', 'Nonprofit with 501C3 IRS Status (Other than an Institution of Higher Education)'),
    LookupType(14, 'N', 'Nonprofit without 501C3 IRS Status (Other than an Institution of Higher Education)'),
    LookupType(15, 'O', 'Private Institution of Higher Education'),
    LookupType(16, 'P', 'Individual'),
    LookupType(17, 'Q', 'For-Profit Organization (Other than Small Business)'),
    LookupType(18, 'R', 'Small Business'),
    LookupType(19, 'S', 'Hispanic-serving Institution'),
    LookupType(20, 'T', 'Historically Black College or University (HBCU)'),
    LookupType(21, 'U', 'Tribally Controlled College or University (TCCU)'),
    LookupType(22, 'V', 'Alaska Native and Native Hawaiian Serving Institutions'),
    LookupType(23, 'W', 'Non-domestic (non-U.S.) Entity'),
    LookupType(24, 'X', 'Other')
]
BUSINESS_TYPE_DICT = {item.name: item.desc for item in BUSINESS_TYPE}

BUSINESS_FUNDS_IND = [
    LookupType(1, 'NON', 'Not Recovery Act'),
    LookupType(2, 'REC', 'Recovery Act')
]
BUSINESS_FUNDS_IND_DICT = {item.name: item.desc for item in BUSINESS_FUNDS_IND}

BUSINESS_CATEGORY_FIELDS = ['airport_authority', 'alaskan_native_owned_corpo', 'alaskan_native_servicing_i',
                            'american_indian_owned_busi', 'asian_pacific_american_own', 'black_american_owned_busin',
                            'c1862_land_grant_college', 'c1890_land_grant_college', 'c1994_land_grant_college',
                            'c8a_program_participant', 'city_local_government', 'community_developed_corpor',
                            'community_development_corp', 'contracting_officers_deter', 'corporate_entity_not_tax_e',
                            'corporate_entity_tax_exemp', 'council_of_governments', 'county_local_government',
                            'domestic_or_foreign_entity', 'domestic_shelter', 'dot_certified_disadvantage',
                            'economically_disadvantaged', 'educational_institution', 'emerging_small_business',
                            'federal_agency', 'federally_funded_research', 'for_profit_organization',
                            'foreign_government', 'foreign_owned_and_located', 'foundation',
                            'hispanic_american_owned_bu', 'hispanic_servicing_institu', 'historically_black_college',
                            'historically_underutilized', 'hospital_flag', 'housing_authorities_public',
                            'indian_tribe_federally_rec', 'inter_municipal_local_gove', 'international_organization',
                            'interstate_entity', 'joint_venture_economically', 'joint_venture_women_owned',
                            'labor_surplus_area_firm', 'limited_liability_corporat', 'local_government_owned',
                            'manufacturer_of_goods', 'minority_institution', 'minority_owned_business',
                            'municipality_local_governm', 'native_american_owned_busi', 'native_hawaiian_owned_busi',
                            'native_hawaiian_servicing', 'nonprofit_organization', 'other_minority_owned_busin',
                            'other_not_for_profit_organ', 'partnership_or_limited_lia', 'planning_commission',
                            'port_authority', 'private_university_or_coll', 'sba_certified_8_a_joint_ve',
                            'school_district_local_gove', 'school_of_forestry', 'self_certified_small_disad',
                            'service_disabled_veteran_o', 'small_agricultural_coopera', 'small_disadvantaged_busine',
                            'sole_proprietorship', 'state_controlled_instituti', 'subchapter_s_corporation',
                            'subcontinent_asian_asian_i', 'the_ability_one_program', 'township_local_government',
                            'transit_authority', 'tribal_college', 'tribally_owned_business', 'us_federal_government',
                            'us_government_entity', 'us_local_government', 'us_state_government',
                            'us_tribal_government', 'veteran_owned_business', 'veterinary_college',
                            'veterinary_hospital', 'woman_owned_business', 'women_owned_small_business']

EXTERNAL_DATA_TYPE = [
    LookupType(1, 'usps_download', 'external data load type for downloading zip files')
]
EXTERNAL_DATA_TYPE_DICT = {item.name: item.id for item in EXTERNAL_DATA_TYPE}
