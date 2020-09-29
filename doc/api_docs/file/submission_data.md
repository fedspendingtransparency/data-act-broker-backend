# GET "/v1/submission\_data/"
This endpoint returns detailed validation job data for the requested submission.

## Sample Request
`/v1/submission_data/?submission_id=123&type=appropriations`

## Request Params
- `submission_id`: (required, integer) the ID of the submission to get job data for
- `type`: (string) limits the results in the array to only contain the given file type. Will return all file types in the submission if not provided. The following are valid values for this:
    - `fabs` - only for FABS submissions
    - `appropriations` - A
    - `program_activity` - B
    - `award_financial` - C
    - `award_procurement` - D1
    - `award` - D2
    - `cross` - cross-file

## Response (JSON)
```
{
    "jobs": [{
        'job_id': 520,
        'job_status': "finished",
        'job_type': "csv_record_validation",
        'filename': "original_file_name.csv",
        'file_size': 1800,
        'number_of_rows': 3,
        'file_type': "fabs",
        'file_status': "complete",
        'error_type': "row_errors",
        'error_data': [{
            'field_name': "recordtype",
            'error_name': "required_error",
            'error_description': "This field is required for all submissions but was not provided in this row.",
            'occurrences': "1",
            'rule_failed': "This field is required for all submissions but was not provided in this row.",
            'original_label': "FABSREQ3"
        }],
        'warning_data': [],
        'missing_headers': [],
        'duplicated_headers': []
    }]
}
```

## Response Attributes
- `job_id `: (int) database ID of the job
- `job_status`: (string) status of the job. Can be any of the following values:
    - `waiting`
    - `ready`
    - `running`
    - `finished`
    - `invalid`
    - `failed`
- `job_type`: (string) the type of validation the job is, can be either of the following values:
    - `csv_record_validation`: a single file validation
    - `validation`: the cross-file validations
- `filename`: (string) the orignal name of the submitted file (null for cross-file)
- `file_size`: (bigint) size of the file in bytes (null for cross-file)
- `number_of_rows`: (integer) total number of rows in the file including header row (null for cross-file)
- `file_type`: (string) type of the file, can only be the following values
    - `fabs` - will be the only file for FABS submissions and will not be present in DABS submissions
    - `appropriations` - A
    - `program_activity` - B
    - `award_financial` - C
    - `award_procurement` - D1
    - `award` - D2
    - ` ` - Empty string is used for cross-file jobs
- `file_status`: (string) the status of the file. Can only be the following values
    - `complete`
    - `header_error`
    - `unknown_error`
    - `single_row_error`
    - `job_error`
    - `incomplete`
    - `encoding_error`
    - `row_count_error`
    - `file_type_error`
- `error_type`: (string) the overall type of error in the validation job. Can only be the following values
    - `header_errors`
    - `row_errors`
    - `none`
- `error_data`: ([dict]) details of each error that ocurred in the submission. Each entry is an dictionary with the following keys, all returned values are strings
    -  `field_name`: (string) the fields that were affected by the rule separated by commas if there are multiple
    -  `error_name`: (string) the name of the error type, can be any of the following values
        -  `required_error`
        -  `rule_failed`
        -  `type_error`
        -  `value_error`
        -  `read_error`
        -  `write_error`
        -  `length_error`
    -  `error_description`: (string) a description of the `error_name`
    -  `occurrences`: (string) the number of times this error ocurred in this file
    -  `rule_failed`: (string) the full description of the rule that failed
    -  `original_label`: (string) the rule label for the rule that failed
-  `warning_data`: ([dict]), details of each warning that ocurred in the submission. Each entry is an dictionary containing the same keys as those found in `error_data` with the exception that `error_name` can only be `rule_failed`.
-  `missing_headers`: ([string]) each entry is a string with the name of the header that was missing
-  `duplicated_headers`: ([string]) each entry is a string with the name of the header that was duplicated

## Errors
Possible HTTP Status Codes:

- 400:
    - Missing `submission_id` parameter
    - Submission does not exist
    - Invalid type parameter
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission