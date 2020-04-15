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
    LookupType(4, 'publishing', 'Submission is being published'),
    LookupType(5, 'reverting', 'Submission is being reverted to certified status')
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
    LookupType(4, 'editfabs', 'This user is allowed to create and edit any FABS data for their agency'),
    LookupType(5, 'fabs', 'This user is allowed to publish any FABS data for their agency')
]
PERMISSION_TYPE_DICT = {item.name: item.id for item in PERMISSION_TYPES[:3]}
ALL_PERMISSION_TYPES_DICT = {item.name: item.id for item in PERMISSION_TYPES}
PERMISSION_TYPE_DICT_ID = {item.id: item.name for item in PERMISSION_TYPES}
PERMISSION_SHORT_DICT = {item.name[0]: item.id for item in PERMISSION_TYPES}
# These are split into DABS and FABS permissions but having DABS permissions gives read-access to FABS submissions
DABS_PERMISSION_ID_LIST = [item.id for item in PERMISSION_TYPES[:3]]
FABS_PERMISSION_ID_LIST = [item.id for item in PERMISSION_TYPES[3:]]
# These are split into groups between DABS and FABS (not to be confused with just DABS writer/submitter)
WRITER_ID_LIST = [item.id for item in PERMISSION_TYPES[1:]]
SUBMITTER_ID_LIST = [item.id for item in [PERMISSION_TYPES[2], PERMISSION_TYPES[4]]]

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

RULE_IMPACT = [
    LookupType(1, 'low', 'low'),
    LookupType(2, 'medium', 'medium'),
    LookupType(3, 'high', 'high')
]
RULE_IMPACT_DICT = {item.name: item.id for item in RULE_IMPACT}
RULE_IMPACT_DICT_ID = {item.id: item.name for item in RULE_IMPACT}

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

DUNS_BUSINESS_TYPE = [
    LookupType(1, 'A6', 'SBA Certified 8(a), Program Participant'),
    LookupType(2, 'JT', 'SBA Certified 8(a), Joint Venture'),
    LookupType(3, 'XX', 'SBA Certified HUBZone Small Business Concern'),
    LookupType(4, 'A7', 'AbilityOne Non Profit Agency'),
    LookupType(5, '2R', 'U.S Federal Government'),
    LookupType(6, '2F', 'U.S. State Government'),
    LookupType(7, '12', 'U.S. Local Government'),
    LookupType(8, '3I', 'Tribal Government'),
    LookupType(9, 'CY', 'Foreign Government'),
    LookupType(10, '20', 'Foreign Owned'),
    LookupType(11, '1D', 'Small Agricultural Cooperative'),
    LookupType(12, 'LJ', 'Limited Liability Company'),
    LookupType(13, 'XS', 'Subchapter S Corporation'),
    LookupType(14, 'MF', 'Manufacturer of Goods'),
    LookupType(15, '2X', 'For Profit Organization'),
    LookupType(16, 'A8', 'Non-Profit Organization'),
    LookupType(17, '2U', 'Other Not For Profit Organization'),
    LookupType(18, 'HK', 'Community Development Corporation Owned Firm'),
    LookupType(19, 'A3', 'Labor Surplus Area Firm'),
    LookupType(20, 'A5', 'Veteran Owned Business'),
    LookupType(21, 'QF', 'Service Disabled Veteran Owned Business'),
    LookupType(22, 'A2', 'Woman Owned Business'),
    LookupType(23, '23', 'Minority Owned Business'),
    LookupType(24, 'FR', 'Asian-Pacific American Owned'),
    LookupType(25, 'QZ', 'Subcontinent Asian (Asian-Indian), American Owned'),
    LookupType(26, 'OY', 'Black American Owned'),
    LookupType(27, 'PI', 'Hispanic American Owned'),
    LookupType(28, 'NB', 'Native American Owned'),
    LookupType(29, 'ZZ', 'Other'),
    LookupType(30, '8W', 'Woman Owned Small Business'),
    LookupType(31, '27', 'Self Certified Small Disadvantaged Business'),
    LookupType(32, 'JX', 'Self Certified HUBZone Joint Venture'),
    LookupType(33, '8E', 'Economically Disadvantaged Women-Owned Small Business'),
    LookupType(34, '8C', 'Joint Venture Women-Owned Small Business'),
    LookupType(35, '8D', 'Economically Disadvantaged Joint Venture Women-Owned Small Business'),
    LookupType(36, 'NG', 'Federal Agency'),
    LookupType(37, 'QW', 'Federally Funded Research and Development Center'),
    LookupType(38, 'C8', 'City'),
    LookupType(39, 'C7', 'County'),
    LookupType(40, 'ZR', 'Inter-municipal'),
    LookupType(41, 'MG', 'Local Government Owned'),
    LookupType(42, 'C6', 'Municipality'),
    LookupType(43, 'H6', 'School District'),
    LookupType(44, 'TW', 'Transit Authority'),
    LookupType(45, 'UD', 'Council of Governments'),
    LookupType(46, '8B', 'Housing Authorities Public/Tribal'),
    LookupType(47, '86', 'Interstate Entity'),
    LookupType(48, 'KM', 'Planning Commission'),
    LookupType(49, 'T4', 'Port Authority'),
    LookupType(50, 'H2', 'Community Development Corporation'),
    LookupType(51, '6D', 'Domestic Shelter'),
    LookupType(52, 'M8', 'Educational Institution'),
    LookupType(53, 'G6', '1862 Land Grant College'),
    LookupType(54, 'G7', '1890 Land Grant College'),
    LookupType(55, 'G8', '1994 Land Grant College'),
    LookupType(56, 'HB', 'Historically Black College or University'),
    LookupType(57, '1A', 'Minority Institution'),
    LookupType(58, '1R', 'Private University or College'),
    LookupType(59, 'ZW', 'School of Forestry'),
    LookupType(60, 'GW', 'Hispanic Servicing Institution'),
    LookupType(61, 'OH', 'State Controlled Institution of Higher Learning'),
    LookupType(62, 'HS', 'Tribal College'),
    LookupType(63, 'QU', 'Veterinary College'),
    LookupType(64, 'G3', 'Alaskan Native Servicing Institution'),
    LookupType(65, 'G5', 'Native Hawaiian Servicing Institution'),
    LookupType(66, 'BZ', 'Foundation'),
    LookupType(67, '80', 'Hospital'),
    LookupType(68, 'FY', 'Veterinary Hospital'),
    LookupType(69, 'HQ', 'DOT Certified DBE'),
    LookupType(70, '05', 'Alaskan Native Corporation Owned Firm'),
    LookupType(71, 'OW', 'American Indian Owned'),
    LookupType(72, 'XY', 'Indian Tribe (Federally Recognized),'),
    LookupType(73, '8U', 'Native Hawaiian Organization Owned Firm'),
    LookupType(74, '1B', 'Tribally Owned Firm'),
    LookupType(75, 'FO', 'Township'),
    LookupType(76, 'TR', 'Airport Authority'),
    LookupType(77, 'G9', 'Other Than One of the Proceeding'),
    LookupType(78, '2J', 'Sole Proprietorship'),
    LookupType(79, '2K', 'Partnership or Limited Liability Partnership'),
    LookupType(80, '2L', 'Corporate Entity (Not Tax Exempt),'),
    LookupType(81, '8H', 'Corporate Entity (Tax Exempt),'),
    LookupType(82, '2A', 'U.S. Government Entity'),
    LookupType(83, 'X6', 'International Organization')
]
DUNS_BUSINESS_TYPE_DICT = {item.name: item.desc for item in DUNS_BUSINESS_TYPE}

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
    LookupType(1, 'usps_download', 'external data load type for downloading zip files'),
    LookupType(2, 'program_activity_upload', 'program activity file loaded into S3')
]
EXTERNAL_DATA_TYPE_DICT = {item.name: item.id for item in EXTERNAL_DATA_TYPE}
