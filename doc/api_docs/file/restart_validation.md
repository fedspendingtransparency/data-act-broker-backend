# POST "/v1/restart\_validation/"
This route alters a submission's jobs' statuses so they are no longer complete (requiring a regeneration and revalidation for all steps), uncaches all generated files, then restarts A/B/C or FABS validations for the specified submission.

## Body (JSON)

```
{
  "submission_id": 1,
  "d2_submission": True
}
```

## Body Description

- `submission_id`: (required, integer) the ID of the submission for which the validations should be restarted.
- `d2_submission`: (boolean) indicates whether this is a DABS or FABS submission (True for FABS), defaults to False when not provided

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

- 400:
    - Submission does not exist
    - Missing `submission_id`
    - Submission is revalidating or publishing
- 401: Login required
- 403: Permission denied, user does not have permission to edit this submission