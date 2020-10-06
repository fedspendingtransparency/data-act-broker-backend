# GET "/v1/get\_significance\_counts/"
This endpoint gets the breakdown of rules and their counts by their categories or significances in a given submission/file, returning the total number of rules and the rule counts.

## Sample Request
`/v1/get_significance_counts/?submission_id=1234&file=A&error_level=warning`

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
    "total_instances": 42349
    "rules": [
        {
            "rule_label": "C23.3",
            "category": "Accuracy",
            "significance": 3,
            "impact": "high",
            "instances": 36,
            "percentage": 0.0
        },
        {
            "rule_label": "C9",
            "category": "Completeness",
            "significance": 4,
            "impact": "low",
            "instances": 73,
            "percentage": 0.1
        },
        {
            "rule_label": "C8",
            "category": "Completeness",
            "significance": 9,
            "impact": "medium",
            "instances": 42240,
            "percentage": 99.7
        }
    ]
}
```

## Response Attributes
The response is a dictionary representing the rules and their significances/categories/counts with the following:

- `total_instances`: (integer) the total number of instances (errors or warnings) for this file in this submission
- `rules `: ([dict]) a detailed breakdown of each rule that is present in this submission/file containing the following keys:
    - `rule_label`: (string) the label of the rule
    - `category`: (string) the category of the rule
    - `significance`: (integer) the significance of the rule
    - `impact`: (string) the impact of the rule
    - `instances`: (integer) the number of times this rule was triggered
    - `percentage`: (float, rounded to first decimal) the percent of times this rule was triggered out of all the rules triggered (using the provided filters)

## Errors
Possible HTTP Status Codes:

- 400:
    - Invalid parameter
    - Missing required parameter
    - FABS submission requested
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission