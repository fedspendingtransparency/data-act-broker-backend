# POST "/v1/historic\_dabs\_summary/"

This route returns a list of submission summary dicts corresponding to the filters provided.
Note: the results will only include the submissions the user has access to based on their MAX permissions.

## Body (JSON)
```
{
    "filters": {
        "quarters": [1, 3],
        "fys": [2017, 2019],
        "agencies": ["089", "1125"]
    }
}
```

## Body Description
- `filters`: (required, dict) used to filter the resulting summaries
    - `quarters`: (required, list[integer]) fiscal year quarters, ranging 1-4, or an empty list to include all.
    - `fys`: (required, list[integer]) fiscal years, ranging from 2017 through the current fiscal year,
              or an empty list to include all.
    - `agencies`: (required, list[string]) CGAC or FREC codes, or an empty list to include all.

## Response (JSON)

```
[
    {
        "agency_name": "Peace Corps (EOP)",
        "submissions": [
            {
                "submission_id": 104,
                "certifier": "Administrator",
                "fy": 2019,
                "quarter": 3
            },
            ...
        ]
    },
    ...
]
```

## Response Attributes
The response is a list of dicts representing the requested agencies and their submission summaries, each with the following attributes:

- `agency_name`:  (dict) the name of the requested agency
- `submissions`: (list) the submissions for that agency in the periods requested, each with the following attributes:
    - `submission_id`: (integer) the submission ID of the summary
    - `certifier`: (string) name of the submission certifier
    - `fy`: (integer) the fiscal year of the summary
    - `quarter`: (integer) the fiscal quarter of the summary

## Errors
Possible HTTP Status Codes:

- 400:
    - Invalid `quarters` parameter
    - Invalid `fys` parameter
    - Invalid `agencies` parameter
    - Missing required parameter
- 401: Login required