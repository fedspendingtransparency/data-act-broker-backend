# GET "/v1/get\_submission\_comments/"
This endpoint retrieves existing submission comments (explanations/notes for particular files).

## Sample Request
`/v1/get_submission_comments/?submission_id=123`

## Request Params
- `submission_id`: (required, string) an integer representing the ID of the submission to get obligations for.

## Response (JSON)
```
{
  "A": "Text of A's comment",
  "B": "These will be empty if no notes are present",
  "C": "",
  "D1": "",
  "D2": "",
  "E": "",
  "F": ""
}
```

## Reponse Attributes
- `A`: (string) comment for file A (Appropriations)
- `B`: (string) comment for file B (Program Activity)
- `C`: (string) comment for file C (Award Financial)
- `D1`: (string) comment for file D1 (Award Procurement)
- `D2`: (string) comment for file D2 (Award Financial Assistance)
- `E`: (string) comment for file E (Executive Compensation)
- `F`: (string) comment for file F (Sub Award)

## Errors
Possible HTTP Status Codes:

- 400:
    - Submission does not exist
    - Missing submission ID parameter
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission