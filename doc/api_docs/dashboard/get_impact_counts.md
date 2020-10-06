# GET "/v1/get\_impact\_counts/"
This endpoint gets the breakdown of each impact level in a given submission/file, returning the total number of rules that fall under each impact level and the details of each rule.

## Sample Request
`/v1/get_impact_counts/?submission_id=1234&file=A&error_level=warning`

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

## Response (JSON)
```
{
    "low": {
        "total": 2,
        "rules": [
            {
                "rule_label": "C9",
                "instances": 72,
                "rule_description": "Lorem ipsum"
            },
            {
                "rule_label": "C23.3",
                "instances": 32,
                "rule_description": "Lorem ipsum"
            }
        ]
    },
    "medium": {
        "total": 0,
        "rules": []
    },
    "high": {
        "total": 1,
        "rules": [
            {
                "rule_label": "C18",
                "instances": 50,
                "rule_description": "Lorem ipsum"
            }
        ]
    }
}
```

## Response Attributes
The response is a dictionary containing three keys, `low`, `medium` and `high` denoting the three levels of impact. Each of these contain a dictionary with a breakdown of the contents of each level. These dictionaries contain:

- `total`: (integer) the total number of rules with this impact level that are present in this submission/file
- `rules `: ([dict]) a detailed breakdown of each rule that is present in this submission/file containing the following keys:
    - `rule_label`: (string) the label of the rule
    - `instances`: (integer) the number of times this rule was triggered
    - `rule_description`: (string) the description of the rule

## Errors
Possible HTTP Status Codes:

- 400:
    - Invalid parameter
    - Missing required parameter
    - FABS submission requested
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission