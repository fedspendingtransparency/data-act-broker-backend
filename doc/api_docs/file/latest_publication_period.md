# GET "/v1/latest\_publication\_period/"
This endpoint returns the latest publication period that has begun for the broker application.

## Sample Request
`/v1/latest_publication_period/`

## Request Params
N/A

## Response (JSON)
```
{
    "period": 7,
    "year": 2019
}
```

## Response Attributes
- `period`: (integer) the period of the latest publication period, or none if no period is found
- `year`: (integer) the fiscal year of the latest publication period, or none if no period is found

## Errors
Possible HTTP Status Codes:

- 401: Login required