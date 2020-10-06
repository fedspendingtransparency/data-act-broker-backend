# GET "/v1/rule\_settings"
This route lists an agency's stored rule settings. 

## Sample Request
`/v1/rule_settings/?agency_code=097&file=cross-AB`

## Request Params
- `agency_code`: (required, string) the CGAC/FREC of the agency
- `file`: (required, string) The file to filter the rule settings. Allowed values are:
    - `A`: Appropriations
    - `B`: Program Activity
    - `C`: Award Financial
    - `cross-AB`: cross-file between Appropriations and Program Activity
    - `cross-BC`: cross-file between Program Activity and Award Financial
    - `cross-CD1`: cross-file between Award Financial and Award Procurement
    - `cross-CD2`: cross-file between Award Financial and Award Financial Assistance

## Response (JSON)

```
{
    "warnings": [
        {
            "description": "The GrossOutlayAmountByTAS_CPE amount in the appropriation file (A) does not equal the sum of the corresponding GrossOutlayAmountByProgramObjectClass_CPE values in the award financial file (B).",
            "label": "A18",
            "significance": 1,
            "impact": "high"
        },
        {
            "description": "The ObligationsIncurredTotalByTAS_CPE amount in the appropriation file (A) does not equal the negative sum of the corresponding ObligationsIncurredByProgramObjectClass_CPE values in the award financial file (B).",
            "label": "A19",
            "significance": 2,
            "impact": "high"
        },
        {
            "description": "DeobligationsRecoveriesRefundsByTAS_CPE in File A should equal USSGL (4871_CPE+ 4971_CPE+ 4872_CPE+ 4972_CPE) for the TAS in File B.",
            "label": "A35",
            "significance": 3,
            "impact": "high"
        }
    ],
    "errors": [
        {
            "description": "All TAS values in File A (appropriations) should exist in File B (object class program activity)",
            "label": "A30.1",
            "significance": 1,
            "impact": "high"
        },
        {
            "description": "All TAS values in File B (object class program activity) should exist in File A (appropriations)",
            "label": "A30.2",
            "significance": 2,
            "impact": "high"
        }
    ]
}
```

## Response Attributes

The response is two dictionaries (`warnings` and `errors`) representing the rule settings, each with a list of dicts with the 
following attributes:
- `label`: (string) the rule number
- `description`: (string) the rule's text
- `impact`:  (string) the impact group. Possible values are:
    - `low`
    - `medium`
    - `high`

## Errors
Possible HTTP Status Codes:

- 400:
    - Invalid parameter
    - Missing required parameter
- 401: Login required
- 403: Permission denied, user does not have permission