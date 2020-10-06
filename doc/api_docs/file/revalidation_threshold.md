# GET "/v1/revalidation\_threshold/"
This endpoint returns the revalidation threshold for the broker application. This is the date that denotes the earliest validation date a submission must have in order to be certifiable.

## Sample Request
`/v1/revalidation_threshold/`

## Request Params
N/A

## Response (JSON)
```
{
    "revalidation_threshold": "01/15/2017"
}
```

## Response Attributes
- `revalidation_threshold`: (string) the date of the revalidation threshold (MM/DD/YYYY)

## Errors
Possible HTTP Status Codes:

- 401: Login required