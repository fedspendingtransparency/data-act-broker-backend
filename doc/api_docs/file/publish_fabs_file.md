# POST "/v1/publish\_fabs\_file/"

This route sends a request to the backend with ID of the FABS submission to publish.

## Body (JSON)
```
{
    "submission_id": 7
}
```

## Body Description

- `submission_id`: (required, integer) ID of the submission to publish

## Response (JSON)
```
{
    "submission_id": 7
}
```

##### Response Attributes

- `submission_id`: (integer) the ID of the submission being published

##### Errors
Possible HTTP Status Codes:

- 400:
    - Invalid submission
    - Already published or currently publishing submission
    - Different submission published the same rows between validation and this API call
    - Different submission that shares valid rows with this one currently publishing
    - Missing required parameter
- 401: Login required
- 403: Permission denied, user does not have permission to publish this submission
- 500: Any other unexpected errors