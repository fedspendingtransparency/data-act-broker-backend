# POST "/v1/revert\_submission/"
This endpoint returns an updated submission to the state it was in at the latest certification.

## Body (JSON)
```
{
    "submission_id": 1234
}
```

## Body Description

- `submission_id`: (required, integer) An integer corresponding to the ID of the submission to be reverted.

## Response (JSON)
```
{
    "message": "Submission 1234 successfully reverted to certified status."
}
```

## Response Attributes
- `message `: (string) A message indicating the submission was successfully reverted

## Errors
Possible HTTP Status Codes:

- 400:
    - Submission does not exist
    - Submission is FABS
    - Submission has never been published or has not been updated since publication
- 401: Login required
- 403: Permission denied, user does not have permission to edit this submission