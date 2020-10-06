# GET "/v1/check\_current\_page/"
This endpoint returns the farthest frontend page a user should be able to see based on the submission's progress.
    
## Sample Request
`/v1/check_current_page/?submission_id=123`
    
## Request Params
- `submission_id`: (required, integer) the ID of the submission whose page is being checked
    
## Response (JSON)
```
{
    "step": "1",
    "message": "This submission is on the /validateData page."
}
```
    
## Response Attributes
- `step`: (string) Which step of the submission the process is on. Values range 1-6 with 6 being `FABS` and 1-5 being steps in a DABS submission.
- `message`: (string) a message indicating in plain English what step of the process the submission is on
    
## Errors
Possible HTTP Status Codes:
    
- 400:
    - Missing `submission_id` parameter
    - Submission does not exist
- 403: Permission denied, user does not have permission to view this submission