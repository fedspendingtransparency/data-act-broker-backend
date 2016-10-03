# This file defines a series of constants that represent the values used in the
# broker's "helper" tables. Rather than define the values in the db setup scripts
# and then make db calls to lookup the surrogate keys, we'll define everything
# here, in a file that can be used by the db setup scripts *and* the application
# code.
# todo: replace getIdFromDict and getNameFromDict baseInterface functions with these constants

from collections import namedtuple

LookupType = namedtuple('LookupType', ['id', 'name', 'desc'])
LookupFileType = namedtuple('LookupFileType', ['id', 'name', 'desc', 'letter', 'order'])

FILE_STATUS = []
FILE_STATUS.append(LookupType(1, 'complete', 'File has been processed'))
FILE_STATUS.append(LookupType(2, 'header_error', 'The file has errors in the header row'))
FILE_STATUS.append(LookupType(3, 'unknown_error', 'An unknown error has occurred with this file'))
FILE_STATUS.append(LookupType(4, 'single_row_error', 'Error occurred in job manager'))
FILE_STATUS.append(LookupType(5, 'job_error', 'File has not yet been validated'))
FILE_STATUS.append(LookupType(6, 'incomplete', 'File has not yet been validated'))
FILE_STATUS_DICT = {item.id: item.name for item in FILE_STATUS}

ERROR_TYPE = []
ERROR_TYPE.append(LookupType(1, 'type_error', 'The value provided was of the wrong type'))
ERROR_TYPE.append(LookupType(2, 'required_error', 'A required value was not provided'))
ERROR_TYPE.append(LookupType(3, 'value_error', 'The value provided was invalid'))
ERROR_TYPE.append(LookupType(4, 'read_error', 'Could not parse this record correctly'))
ERROR_TYPE.append(LookupType(5, 'write_error', 'Could not write this record into the staging table'))
ERROR_TYPE.append(LookupType(6, 'rule_failed', 'A rule failed for this value'))
ERROR_TYPE.append(LookupType(7, 'length_error', 'Value was longer than allowed length'))
ERROR_TYPE_DICT = {item.id: item.name for item in ERROR_TYPE}

JOB_STATUS = []
JOB_STATUS.append(LookupType(1, 'waiting', 'check dependency table'))
JOB_STATUS.append(LookupType(2, 'ready', 'can be assigned'))
JOB_STATUS.append(LookupType(3, 'running', 'job is currently in progress'))
JOB_STATUS.append(LookupType(4, 'finished', 'job is complete'))
JOB_STATUS.append(LookupType(5, 'invalid', 'job is invalid'))
JOB_STATUS.append(LookupType(6, 'failed', 'job failed to complete'))
JOB_STATUS_DICT = {item.id: item.name for item in JOB_STATUS}

JOB_TYPE = []
JOB_TYPE.append(LookupType(1, 'file_upload', 'file must be uploaded to S3'))
JOB_TYPE.append(LookupType(2, 'csv_record_validation', 'do record level validation and add to staging table'))
JOB_TYPE.append(LookupType(3, 'db_transfer', 'information must be moved from production DB to staging table'))
JOB_TYPE.append(LookupType(4, 'validation', 'new information must be validated'))
JOB_TYPE.append(LookupType(5, 'external_validation', 'new information must be validated against external sources'))
JOB_TYPE_DICT = {item.id: item.name for item in JOB_TYPE}

PUBLISH_STATUS = []
PUBLISH_STATUS.append(LookupType(1, 'unpublished', 'Has not yet been moved to data store'))
PUBLISH_STATUS.append(LookupType(2, 'published', 'Has been moved to data store'))
PUBLISH_STATUS.append(LookupType(3, 'updated', 'Submission was updated after being published'))
PUBLISH_STATUS_DICT = {item.id: item.name for item in PUBLISH_STATUS}

FILE_TYPE = []
FILE_TYPE.append(LookupFileType(1, 'appropriations', '', 'A', 1))
FILE_TYPE.append(LookupFileType(2, 'program_activity', '', 'B', 2))
FILE_TYPE.append(LookupFileType(3, 'award_financial', '', 'C', 3))
FILE_TYPE.append(LookupFileType(4, 'award', '', 'D2', 4))
FILE_TYPE.append(LookupFileType(5, 'award_procurement', '', 'D1', 5))
FILE_TYPE.append(LookupFileType(6, 'awardee_attributes', '', 'E', None))
FILE_TYPE.append(LookupFileType(7, 'sub_award', '', 'F', None))
FILE_TYPE_DICT = {item.id: item.name for item in FILE_TYPE}

USER_STATUS = []
USER_STATUS.append(LookupType(1, 'awaiting_confirmation', 'User has entered email but not confirmed'))
USER_STATUS.append(LookupType(2, 'email_confirmed', 'User email has been confirmed'))
USER_STATUS.append(LookupType(3, 'awaiting_approval', 'User has registered their information and is waiting for approval'))
USER_STATUS.append(LookupType(4, 'approved', 'User has been approved'))
USER_STATUS.append(LookupType(5, 'denied', 'User registration was denied'))
USER_STATUS_DICT = {item.id: item.name for item in USER_STATUS}

PERMISSION_TYPE = []
PERMISSION_TYPE.append(LookupType(0, 'agency_user', 'This user is allowed to upload data to be validated'))
PERMISSION_TYPE.append(LookupType(1, 'website_admin', 'This user is allowed to manage user accounts'))
PERMISSION_TYPE.append(LookupType(2, 'agency_admin', 'This user is allowed to manage user accounts within their agency'))
PERMISSION_TYPE_DICT = {item.id: item.name for item in PERMISSION_TYPE}

FIELD_TYPE = []
FIELD_TYPE.append(LookupType(1, 'INT', 'integer type'))
FIELD_TYPE.append(LookupType(2, 'DECIMAL', 'decimal type '))
FIELD_TYPE.append(LookupType(3, 'BOOLEAN', 'yes/no'))
FIELD_TYPE.append(LookupType(4, 'STRING', 'string type'))
FIELD_TYPE.append(LookupType(5, 'LONG', 'long integer'))
FIELD_TYPE_DICT = {item.id: item.name for item in FIELD_TYPE}

RULE_SEVERITY = []
RULE_SEVERITY.append(LookupType(1, 'warning', 'warning'))
RULE_SEVERITY.append(LookupType(2, 'fatal', 'fatal error'))
RULE_SEVERITY_DICT = {item.id: item.name for item in RULE_SEVERITY}
