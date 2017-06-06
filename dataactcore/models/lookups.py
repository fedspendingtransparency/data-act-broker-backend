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
    LookupType(7, 'encoding_error', 'File contains invalid characters that could not be validated')
]
FILE_STATUS_DICT = {item.name: item.id for item in FILE_STATUS}
FILE_STATUS_DICT_ID = {item.id: item.name for item in FILE_STATUS}

ERROR_TYPE = [
    LookupType(1, 'type_error', 'The value provided was of the wrong type'),
    LookupType(2, 'required_error', 'A required value was not provided'),
    LookupType(3, 'value_error', 'The value provided was invalid'),
    LookupType(4, 'read_error', 'Could not parse this record correctly'),
    LookupType(5, 'write_error', 'Could not write this record into the staging table'),
    LookupType(6, 'rule_failed', 'A rule failed for this value'),
    LookupType(7, 'length_error', 'Value was longer than allowed length')
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
    LookupType(3, 'db_transfer', 'information must be moved from production DB to staging table'),
    LookupType(4, 'validation', 'new information must be validated'),
    LookupType(5, 'external_validation', 'new information must be validated against external sources')
]
JOB_TYPE_DICT = {item.name: item.id for item in JOB_TYPE}
JOB_TYPE_DICT_ID = {item.id: item.name for item in JOB_TYPE}

PUBLISH_STATUS = [
    LookupType(1, 'unpublished', 'Has not yet been moved to data store'),
    LookupType(2, 'published', 'Has been moved to data store'),
    LookupType(3, 'updated', 'Submission was updated after being published')
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
    LookupFileType(8, 'detached_award', '', 'D2_detached', None, False, DetachedAwardFinancialAssistance)
]
FILE_TYPE_DICT = {item.name: item.id for item in FILE_TYPE}
FILE_TYPE_DICT_ID = {item.id: item.name for item in FILE_TYPE}
FILE_TYPE_DICT_LETTER = {item.id: item.letter for item in FILE_TYPE}
FILE_TYPE_DICT_LETTER_ID = {item.letter: item.id for item in FILE_TYPE}
FILE_TYPE_DICT_LETTER_NAME = {item.letter: item.name for item in FILE_TYPE}

PERMISSION_TYPE = [
    LookupType(1, 'reader', 'This user is allowed to view any submission for their agency'),
    LookupType(2, 'writer', 'This user is allowed to create and edit any submission for their agency'),
    LookupType(3, 'submitter', 'This user is allowed to certify and submit any submission for their agency'),
    # Placeholder 4: website_admin
]
PERMISSION_TYPE_DICT = {item.name: item.id for item in PERMISSION_TYPE}
PERMISSION_TYPE_DICT_ID = {item.id: item.name for item in PERMISSION_TYPE}
PERMISSION_SHORT_DICT = {item.name[0]: item.id for item in PERMISSION_TYPE}

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
