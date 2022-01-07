# GET "/v1/get\_submission\_zip/"
This endpoint retrieves the url to the zip containing all the essential files attached to this submission.
Note only DABS submissions can be zipped.

## Sample Request
`/v1/get_submission_zip?submission_id=123&publish_history_id=1`

## Request Params
- `submission_id`: (required, integer) the ID of the submission to get the zip for. 
- `publish_history_id`: (required, integer) the ID of the publish history to zip

## Response (JSON)
```
{
    "url": "http://url.to.file/full/path.csv"
}
```

## Response Attributes
- `url`: (string) the url to the submission zip

## Errors
Possible HTTP Status Codes:

- 400:
    - Missing `submission_id` parameter
    - Missing/invalid `publish_history_id` parameter
    - Submission does not exist
    - Submission is a FABS submission
    - A submission file has been removed since publishing 
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission