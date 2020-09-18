# GET "/v1/submission/\<int:submission\_id\>/report\_url/"
This endpoint requests the URL associated with a particular type of submission report. The provided URL will expire after one minute.

## Sample Request
`/v1/submission/1234/report_url/?warning=True&file_type=appropriations&cross_type=award_financial`

## Request Params
- `submission_id`: (required, integer) the ID of the submission to get a report url for
- `file_type`: (required, string) the type of report being requested
    - `appropriations` - A
    - `program_activity` - B
    - `award_financial` - C
    - `award_procurement` - D1
    - `award` - D2
    - `fabs` - FABS
- `warning`: (boolean) true if the report is a warning report. Defaults to false
- `cross_type`: (string) if present, indicates that we're looking for a cross-validation report between `file_type` and this parameter. The following are the only valid pairings, all other combinations of `file_type` and `cross_type` will result in an error:
    - `file_type`: "appropriations", `cross_type`: "program\_activity"
    - `file_type`: "program\_activity", `cross_type`: "award\_financial"
    - `file_type`: "award\_financial", `cross_type`: "award\_procurement"
    - `file_type`: "award\_financial", `cross_type`: "award" 

## Response (JSON)

```
{
  "url": "https://........"
}
```

## Response Attributes
- `url` - signed url for the submission report

## Errors
Possible HTTP Status Codes:

- 400:
    - Missing `submission_id` or `file_type` parameter
    - Submission does not exist
    - Invalid `file_type`, `cross_type`, or `warning` parameter
    - Invalid `file_type`, `cross_type` pairing
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission