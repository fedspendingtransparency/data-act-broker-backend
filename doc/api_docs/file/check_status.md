# GET "/v1/check\_status/"
This endpoint returns the status of each file type, including whether each has errors or warnings and a message if one exists.

## Sample Request
`/v1/check_status/?submission_id=123&type=appropriations`

## Request Params
- `submission_id`: (required, integer) represents the ID of the submission to get statuses for
- `type`: (string) limits the results in the array to only contain the given file type. The following are valid values for this:
    - `fabs` - only for FABS submissions
    - `appropriations` - A
    - `program_activity` - B
    - `award_financial` - C
    - `award_procurement` - D1
    - `award` - D2
    - `cross` - cross-file
    - `executive_compensation` - E
    - `sub_award` - F

## Response (JSON)
```
{
    "fabs": {
        "status": "finished",
        "message": "",
        "has_errors": false,
        "has_warnings": true,
        "upload_progress": 78.4,
        "validation_progress": null
    }
}
```

## Response Attributes
Response attributes change depending on the submission and type requested. If a specific type is requested, only one attribute matching the requested type will be included. If no type is specified and the submission is a DABS submission, all possible file types will be included. The possible attributes match the valid request types. See above for a full list.

The contents of each attribute are an dictionary containing the following keys:

- `status`: (string) indicates the current status of the file type. Possible values include:
    - `ready`: not yet started
    - `uploading`: the file is uploading
    - `running`: the jobs are running
    - `finished`: all associated jobs are complete
    - `failed`: one or more of the associated jobs have failed
- `message`: (string) the message associated with a job if there is one
- `has_errors`: (boolean) indicates if the file type has any errors in validation
- `has_warnings`: (boolean) indicates if the file type has any warnings in validation
- `upload_progress`: (float) indicates the percent progress of the upload jobs of that type. If there are no upload jobs associated with the job type it will display `null`
- `validation_progress`: (float) indicates the percent progress of the validation jobs of that type. If there are no validation jobs associated with the job type it will display `null`

## Errors
Possible HTTP Status Codes:

- 400:
    - Missing `submission_id` parameter
    - Submission does not exist
    - Invalid type parameter
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission