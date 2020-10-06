# GET "/v1/published\_submissions/"
This endpoint returns a list of published submissions for a given agency and fiscal period or quarter.

## Sample Request
`/v1/published\_submissions/?cgac_code=&frec_code=1601&reporting_fiscal_year=2020&reporting_fiscal_period=12&is_quarter=true`

## Request Params
- `cgac_code`: (required if not FREC, string) CGAC of agency (null if FREC agency)
- `frec_code`: (required if not CGAC, string) FREC of agency (null if CGAC agency)
- `is_quarter`: (boolean) whether or not a new submission being made in this time will be quarterly or not (True for quarterly submissions)
- `reporting_fiscal_year`: (string) the fiscal year to check for published submissions
- `reporting_fiscal_period`: (string) the fiscal period to check for published submissions

## Response (JSON)
```
{
    "published_submissions": [
        {
            "submission_id": 123,
            "is_quarter": false
        },
        {
            "submission_id": 234,
            "is_quarter": false
        },
    ]
}
```

## Response Attributes
- `published_submissions`: ([dict]) each dictionary represents a published submission in the requested agency and period. 
    - `submission_id`: (integer) an integer representing the ID of the submission
    - `is_quarter`: (boolean) whether or not it is a quarterly submission

## Errors
Possible HTTP Status Codes:

- 400: CGAC or FREC code not provided
- 401: Login required
