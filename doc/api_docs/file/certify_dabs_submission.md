# POST "/v1/certify\_dabs\_submission/"
This route certifies the specified submission, if possible. A submission that has not been published cannot be certified. If the submission is quarterly, has been certified before, or it is past the certification deadline for the submission it must be published and certified at the same time. For these cases, use `publish_and_certify_dabs_submission`

## Body (JSON)

```
{
  "submission_id": 1
}
```

## Body Description
- `submission_id`: (required, integer) the ID of the submission that is to be certified.

## Response (JSON)

```
{
  "message": "Success"
}
```

## Response Attributes
- `message`: (string) A message indicating whether or not the action was successful. Any message other than "Success" indicates a failure.

## Errors
Possible HTTP Status Codes:

- 400
  - Submission does not exist
  - Critical errors prevent the submission from being published
  - Submission is not published
  - Submission is already certified
  - Submission is a quarterly submission
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission