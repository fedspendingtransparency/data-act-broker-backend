# GET "/v1/active\_submission\_overview/"
This endpoint returns an overview of the requested submission, along with details about the errors and/or warnings associated

## Sample Request
`/v1/active_submission_overview/?submission_id=123&file=B&error_level=warning`

## Request Params

- `submission_id`: (required, integer) the ID of the submission to view
- `file`: (required, string) The file to get the warning or error data for. Allowed values are:
    - `A`: Appropriations
    - `B`: Program Activity
    - `C`: Award Financial
    - `cross-AB`: cross-file between Appropriations and Program Activity
    - `cross-BC`: cross-file between Program Activity and Award Financial
    - `cross-CD1`: cross-file between Award Financial and Award Procurement
    - `cross-CD2`: cross-file between Award Financial and Award Financial Assistance
- `error_level`: (string) The level of error data to gather an overview for. Defaults to warning. Allowed values:
    - `warning`
    - `error`
    - `mixed`

## Response (JSON)
```
{
    "submission_id": 1234,
    "icon_name": "ABC.jpg",
    "agency_name": "Fake Agency (FAKE)"
    "certification_deadline": "January 1, 2020",
    "days_remaining": 10,
    "reporting_period": "FY 20 / Q1",
    "duration": "Quarterly",
    "file": "File B",
    "number_of_rules": 8,
    "total_instances": 12345
}
```

## Response Attributes
- `submission_id`: (integer) The ID of the submission
- `icon_name`: (string) The name of the icon (if one exists, else null) associated with the agency.
- `agency_name`: (string) The name of the agency associated with the submission.
- `certification_deadline`: (string) The day the certification for this submission is due. `Past Due` for submissions whose deadline has passed and Null for test submissions.
- `days_remaining`: (integer/string) The number of days left until the certification deadline. `Due Today` for submissions that are due that day, null for submissions whose deadline has passed and test submissions.
- `reporting_period`: (string) The reporting period of the submission. Formatted `FY # / Q#` for quarterly submissions and `MM/YYYY` for monthly submissions.
- `duration`: (string) The type of submission it is. Possible values:
    - `Quarterly`
    - `Monthly`
- `file`: (string) The name of the file being looked at. Possible values:
    - `A`: Appropriations
    - `B`: Program Activity
    - `C`: Award Financial
    - `cross-AB`: cross-file between Appropriations and Program Activity
    - `cross-BC`: cross-file between Program Activity and Award Financial
    - `cross-CD1`: cross-file between Award Financial and Award Procurement
    - `cross-CD2`: cross-file between Award Financial and Award Financial Assistance
- `number_of_rules`: (integer) The total number of rules that have been violated in this submission, matching the severity and file indicated.
- `total_instances`: (integer) The total number of times rules were violated in this submission, matching the severity and file indicated.

## Errors
Possible HTTP Status Codes:

- 400:
    - Invalid parameter
    - Missing required parameter
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission