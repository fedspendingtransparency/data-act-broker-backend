# GET "/v1/get\_obligations/"
This endpoint gets the obligation sums for the given submission.

## Sample Request
`/v1/get_obligations/?submission_id=123`

## Request Params
- `submission_id`: (required, integer) the ID of the submission to get obligations for

## Response (JSON)

```
{
  "total_obligations": 75000.01,
  "total_procurement_obligations": 32500.01,
  "total_assistance_obligations": 42500
}
```

## Reponse Attributes
- `total_obligations`: (float) the total obligations for the requested submission
- `total_procurement_obligations`: (float) the total procurement obligations for the requested submission
- `total_assistance_obligations`: (float) the total assistance obligations for the requested submission

## Errors
Possible HTTP Status Codes:

- 400:
    - Missing `submission_id` parameter
    - Submission does not exist
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission