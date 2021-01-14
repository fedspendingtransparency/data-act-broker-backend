# POST "/v1/update\_submission\_comments/"
This endpoint sets the file comments for a given submission.

## Body (JSON)

```
{
  "submission_id": 1234,
  "A": "Some new text",
  "C": "We didn't include B",
  "D1": "",
  "D2": "",
  "F": "Or E, for some reason"
}
```

## Body Description
All content passed in the body is updated in the database. If an attribute is left out, it will be treated as if it's an empty string.

**Important:** All comments must be included every time in order to be kept. An attribute with an empty string will result in that comment being deleted. (e.g. A comment for file A already exists. A comment for file B is being added. Comments for both files A and B must be sent).

- `submission_id`: (required, string) The ID of the submission whose comments are getting updated
- `A`: (string) comment for file A (Appropriations)
- `B`: (string) comment for file B (Program Activity)
- `C`: (string) comment for file C (Award Financial)
- `D1`: (string) comment for file D1 (Award Procurement)
- `D2`: (string) comment for file D2 (Award Financial Assistance)
- `E`: (string) comment for file E (Executive Compensation)
- `F`: (string) comment for file F (Sub Award)

## Response (JSON)

```
{}
```

## Response Attributes
N/A

## Errors
Possible HTTP Status Codes:

- 400:
    - Submission does not exist
    - Missing submission ID parameter
- 401: Login required
- 403: Permission denied, user does not have update to view this submission