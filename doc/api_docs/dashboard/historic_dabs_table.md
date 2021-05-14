# POST "/v1/historic\_dabs\_table/"
This route returns a list of warning metadata rows for the dashboard table. Filters allow for more refined lists.
Note: the results will only include the submissions the user has access to based on their MAX permissions.

## Body (JSON)
```
{
    "filters": {
        "periods": [1, 3],
        "fys": [2017, 2019],
        "agencies": ["089", "1125"],
        "files": ["B"],
        "rules": ["B11.4", "B9"],
        "is_quarter": False
    },
    "page": 1,
    "limit": 10,
    "sort": "rule_label",
    "order": "asc"
}
```

## Body Description
- `filters`: (required, dict) used to filter the resulting error metadata list
    - `periods`: (required, list[integer]) fiscal year periods, ranging 2-12, or an empty list to include all (must be multiples of 3 if `is_quarter` is `True`.
    - `fys`: (required, list[integer]) fiscal years, ranging from 2017 through the current fiscal year,
              or an empty list to include all.
    - `agencies`: (required, list[string]) CGAC or FREC codes, or an empty list to include all.
    - `files`: (required, list[string]) files, or an empty list to include all.
    - `rules`: (required, list[string]) validation rules, or an empty list to include all.
    - `is_quarter`: (required, bool) a flag indicating whether or not to include all periods that would fall under the quarter a provided period encompasses. When this is `True`, if periods are provided they must be multiples of 3.
- `page`: (integer) The page of warning metadata to view (offsets the list by `limit * (page - 1)`). Defaults to `1` if not provided
- `limit`: (integer) The total number of results to see from this request. Defaults to `5` if not provided
- `sort`: (string) What value to sort by. Defaults to `period` if not provided. NOTE: Each sort value has a secondary (and sometimes tertiary) sort value to break ties. Valid values are:
    - `period` - fiscal year/period (secondary: rule label)
    - `rule_label` - the label of the rule (e.g. `B9`) (secondary: period)
    - `instances` - the number of times this warning occurred in this submission/file (secondary: submission ID, tertiary: rule label)
    - `description` - the description of the rule (secondary: submission ID)
    - `submission_id` - the ID of the submission (secondary: rule label)
    - `submitted_by` - the latest user to publish the submission (secondary: submission_id, tertiary: rule label)
- `order`: (string) The sort order. Defaults to `desc` if not provided. Valid values are:
    - `desc`
    - `asc`

## Response (JSON)

```
{
    "results": [
        {
            "submission_id": 1234,
            "files": ["B",  "C"],
            "fy": 2017,
            "period": 3,
            "rule_label": "B9",
            "instance_count": 609,
            "rule_description": "lorem ipsum whatever",
            "submitted_by": "User 1"
        },
        {
            "submission_id": 1234,
            "files": ["B"],
            "fy": 2017,
            "period": 3,
            "rule_label": "B9",
            "instance_count": 609,
            "rule_description": "lorem ipsum whatever",
            "submitted_by": "User 1"
        },
        ...
    ],
    "page_metadata": {
        "total": 20,
        "page": 1,
        "limit": 10
    }
```

## Response Attributes

The response is a dictionary containing a list with the details of each warning with the following attributes and a metadata dictionary containing information pertaining to the table in general:

- `results`: ([dict]) the list of row results
    - `submission_id`: (integer) the submission ID of the warning
    - `files`:  ([string]) The file types associated with the warning
    - `fy`: (integer) the fiscal year of the warning
    - `period`: (integer) the fiscal period of the warning
    - `rule_label`: (string) the label associated with the warning
    - `instance_count`: (integer) the number of times the warning occurred within the submission/file
    - `rule_description`: (string) the text of the warning
    - `submitted_by`: (string) the user that most recently published the submission
- `page_metadata`: (dict) metadata associated with the table
    - `total`: (integer) total number of warning metadata rows these filters found
    - `page`: (integer) the current page requested by the user
    - `limit`: (integer) the total number of results to display per page as requested by the user


## Errors
Possible HTTP Status Codes:

- 400:
    - Invalid parameter
    - Missing required parameter
- 401: Login required