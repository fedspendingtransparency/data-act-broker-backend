# GET "/v1/get\_comments\_file/"
This endpoint retrieves the url to the file containing all the file comments associated with this submission.

## Sample Request
`/v1/get_comments_file/?submission_id=123`

## Request Params
- `submission_id`: (required, integer) the ID of the submission to get the comments file for. 

## Response (JSON)
```
{
    "url": "http://url.to.file/full/path.csv"
}
```

## Response Attributes
- `url`: (string) the url to the comments file

## Errors
Possible HTTP Status Codes:

- 400:
    - Missing `submission_id` parameter
    - Submission does not exist
    - Submission does not have any comments associated with it
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission