# POST "/v1/historic\_dabs\_graphs/"

This route returns a list of submission graph dicts corresponding to the filters provided.
Note: the results will only include the submissions the user has access to based on their MAX permissions.

## Body (JSON)
```
{
    "filters": {
        "periods": [1, 3],
        "fys": [2017, 2019],
        "agencies": ["089", "1125"],
        "files": ["B"],
        "rules": ["B11.4", "B9"]
    }
}
```

## Body Description
- `filters`: (required, dict) used to filter the resulting summaries
    - `periods`: (required, list[integer]) fiscal year periods, ranging 2-12, or an empty list to include all.
    - `fys`: (required, list[integer]) fiscal years, ranging from 2017 through the current fiscal year,
              or an empty list to include all.
    - `agencies`: (required, list[string]) CGAC or FREC codes, or an empty list to include all.
    - `files`: (required, list[string]) files, or an empty list to include all.
    - `rules`: (required, list[string]) validation rules, or an empty list to include all.

## Response (JSON)

```
{
    "B": [
        {
            "submission_id": 1234,
            "agency": 097,
            "fy": 2017,
            "period": 3,
            "is_quarter": True,
            "total_warnings": 519,
            "warnings": [
                {
                    "label": "B11.4",
                    "instances": 352,
                    "percent_total": 68
                }, {
                    "label": "B9",
                    "instances": 167,
                    "percent_total": 32
                }
            ]
        },
        ...
    ],
    "C": [
        {
            "submission_id": 1234,
            "agency": 012,
            "fy": 2017,
            "period": 3,
            "is_quarter": True,
            "total_warnings": 389,
            "warnings": [
                 {
                     "label": "C12",
                     "instances": 389,
                     "percent_total": 100
                 }
            ]
        },
        ...
    ],
```

## Response Attributes

The response is a dictionary of lists representing the submission graphs, each with a list of dicts with the following attributes:

- `submission_id`: (integer) the submission ID of the summary
- `agency`:  (dict) the submission's agency, with the following attributes
    - `name`: (string) the agency's name
    - `code`: (string) the agency's code
- `fy`: (integer) the fiscal year of the summary
- `period`: (integer) the fiscal period of the summary
- `is_quarter`: (boolean) whether the submission is monthly or quarterly (True for quarterly)
- `total_warnings`: (integer) the total instances of warnings associated with this submission and file
- `filtered_warnings`: (integer) the total instances of warnings that fit the rule filters for the request
- `warnings`: ([dict]) list of warning dicts with the following attributes:
    - `label`: (string) rule number/label
    - `instances`: (integer) instances of this specific warning for this file and submission
    - `percent_total`: (integer) the percent of instances for this warning compared to the rest of the file and submission


## Errors
Possible HTTP Status Codes:

- 400:
    - Invalid parameter
    - Missing required parameter
- 401: Login required