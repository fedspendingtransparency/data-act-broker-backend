# POST "/v1/publish\_dabs\_submission/"
This route publishes the specified submission, if possible. A submission with critical errors cannot be published. If the submission is quarterly, has been certified before, or it is past the certification deadline for the submission it must be published and certified at the same time. For these cases, use [publish\_and\_certify\_dabs\_submission](./publish_and_certify_dabs_submission.md)

## Body (JSON)

```
{
  "submission_id": 1
}
```

## Body Description
- `submission_id`: (required, integer) the ID of the submission that is to be published.

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
  - Submission is a test submission
  - Submission is already published and not updated
  - Submission is already certified
  - A validation was completed before the revalidation threshold or the start of the submission window for the submission's year/quarter
  - Submission window for this year/period doesn't exist
  - A different submission for this period was already published
  - File A or B is blank
  - Submission is a quarterly submission
  - It is past the certification window for the submission
- 401: Login required
- 403: Permission denied, user does not have permission to publish this submission