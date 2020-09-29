# POST "/v1/delete\_submission/"

This route deletes all data and files related to the specified `submission_id`. A submission that has ever been certified/published (has a status other than "unpublished") cannot be deleted.

**NOTE**: This is permanent, there is no way to undo it.

## Body (JSON)

```
{
  "submission_id": 1
}
```

## Body Description

- `submission_id`: (required, integer) the ID of the submission that is to be deleted.

## Response (JSON)

```
{
  "message": "Success"
}
```

## Response Attributes
- `message`: (string) indicates whether or not the action was successful. Any message other than "Success" indicates a failure.

## Errors
Possible HTTP Status Codes:
    
- 400:
    - Missing `submission_id`
    - Invalid `submission_id`
    - Submission is not unpublished
    - Submission still has jobs running
- 401: Login required
- 403: Permission denied, user does not have permission to edit this submission