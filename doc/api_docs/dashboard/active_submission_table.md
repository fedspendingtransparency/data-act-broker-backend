# GET "/v1/active\_submission\_table/"
This endpoint returns a dictionary containing metadata about a table and a set of individual table rows for the active DABS dashboard

## Sample Request
`/v1/active_submission_table/?submission_id=123&file=B&error_level=warning&page=1&limit=10&sort=significance&order=desc`

## Request Params

- `submission_id`: (required, integer) the ID of the submission to view
- `file`: (required, string) The file to get the warning or error data for. Allowed values are:
    - `A`: Appropriations
    - `B`: Program Activity
    - `C`: Award Financial
    - `cross-AB`: cross-file between Appropriations and Program Activity
    - `cross-BC`: cross-file between Program Activity and Award Financial
    - `cross-CD1`: cross-file between Award Financial and Award Procurement
    - `cross-CD2`: cross-file between Award Financial and Award Financial Assistance
- `error_level`: (string) The level of error data to gather an overview for. Defaults to warning. Allowed values:
    - `warning`
    - `error`
    - `mixed`
- `page`: (integer) the page of submissions to view (offsets the list by `limit * (page - 1)`). Defaults to `1` if not provided
- `limit`: (integer) the total number of results to see from this request. Defaults to `5` if not provided
- `sort`: (string) What value to sort by. Defaults to `significance` if not provided. NOTE: Some sort values have a secondary sort value to break ties. Valid values are:
    - `significance` - significance
    - `rule_label` - the label of the rule (e.g. `B9`)
    - `instances` - the number of times this rule occurred in this submission/file (secondary: significance)
    - `category` - the category of the rule (secondary: significance)
    - `impact` - the impact specified for this rule (secondary: significance)
    - `description` - the description of the rule
- `order`: (string) the sort order. Defaults to `asc` if not provided. Valid values are:
    - `desc`
    - `asc`

## Response (JSON)
```
{
    "results": [
        {
            "significance": 1,
            "rule_label": "C8",
            "instance_count": 609,
            "category": "Completeness",
            "impact": "High",
            "rule_description": "lorem ipsum whatever"
        },
        {
            "significance": 2,
            "rule_label": "C9",
            "instance_count": 543,
            "category": "Accuracy",
            "impact": "Low",
            "rule_description": "lorem ipsum dolor"
        },
        ...
    ],
    "page_metadata": {
        "total": 20,
        "page": 1,
        "limit": 10,
        "submission_id": 1234,
        "files": ["B", "C"]
    }
}
```

## Response Attributes
The response is a dictionary containing a list with the details of each warning with the following attributes and a metadata dictionary containing information pertaining to the table in general:

- `results`: ([dict]) the list of row results
    - `significance `: (integer) the significance of the rule as defined by the agency
    - `rule_label`: (string) the label associated with the rule
    - `instance_count`: (integer) the number of times the rule occurred within the submission/file
    - `category`: (string) the category associated with the rule. Possible values are:
        - `Completeness`
        - `Accuracy`
        - `Existence`
    - `impact`: (string) the impact the rule has as defined by the agency. Possible values are:
        - `Low`
        - `Medium`
        - `High`
    - `rule_description`: (string) the text of the rule
- `page_metadata`: (dict) metadata associated with the table
    - `total`: (integer) total number of metadata rows these filters found
    - `page`: (integer) the current page requested by the user
    - `limit`: (integer) the total number of results to display per page as requested by the user
    - `submission_id`: (integer) the submission ID selected
    - `files`: ([string]) The file type(s) requested (one for single file, two for cross file)

## Errors
Possible HTTP Status Codes:

- 400: 
    - Invalid submission
    - Invalid parameter
    - Missing required parameter
    - FABS submission requested
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission