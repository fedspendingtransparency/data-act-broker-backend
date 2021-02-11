# GET "/v1/get\_fabs\_meta/"
This endpoint returns metadata about a requested FABS submission
    
## Sample Request
`/v1/get_fabs_meta/?submission_id=123`
    
## Request Params
- `submission_id`: (required, int) the ID of the submission to get metadata for
    
## Response (JSON)
```
{
    "valid_rows": 1234,
    "total_rows": 1567,
    "publish_date": "2019-03-24T14:52:09"
    "published_file": "https://..."
}
```
    
## Response Attributes
- `valid_rows`: (integer) the number of rows in the FABS submission that passed validations without errors
- `total_rows `: (integer) the total number of rows in the FABS submission
- `publish_date`: (string) the publication date of the submission in the format `YYYY/MM/DDThh:mm:ss`. Null if it is not a published submission
- `published_file`: The link to the published FABS file, which includes the derivations for each row. Null if it is not a published submission
    
## Errors
Possible HTTP Status Codes:
    
- 400:
    - Missing `submission_id` parameter
    - Submission does not exist
- 403: Permission denied, user does not have permission to view this submission