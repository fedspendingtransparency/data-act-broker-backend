# POST "/v1/save\_rule\_settings"
This route saves an agency's rule settings. Note that all the rules associated with the file type and error type must be sent together. Additionally, the order of them determines their significance.

## Body (JSON)
```
{
	"agency_code": "097",
	"file": "cross-BC",
    "warnings": [
        {
            "label": "C21",
            "impact": "medium"
        },
        {
            "label": "C20",
            "impact": "low"
        }
    ],
    "errors": [
        {
            "label": "B20",
            "impact": "medium"
        }
    ]
}
```

## Body Description
- `agency_code`: (required, string) the CGAC/FREC of the agency
- `file`: (required, string) The file to filter the rule settings. Allowed values are:
    - `A`: Appropriations
    - `B`: Program Activity
    - `C`: Award Financial
    - `cross-AB`: cross-file between Appropriations and Program Activity
    - `cross-BC`: cross-file between Program Activity and Award Financial
    - `cross-CD1`: cross-file between Award Financial and Award Procurement
    - `cross-CD2`: cross-file between Award Financial and Award Financial Assistance
- `errors`: (required, [dict]) The settings of the agency's errors for that file. *Note: the order of this determines the significance.* Comprised of the following:
    - `label`: (required, string) the label of the rule
    - `impact`: (required, string) the new impact of the rule
- `warnings`: (required, [dict]) The settings of the agency's warnings for that file. *Note: the order of this determines the significance.* Comprised of the following:
    - `label`: (required, string) the label of the rule
    - `impact`: (required, string) the new impact of the rule

## Response (JSON)

```
{
    "message": "Agency 097 rules saved."
}
```

## Response Attributes

- `message`: (string) success message stating which agency's rules have been updated

## Errors
Possible HTTP Status Codes:

- 400:
    - Invalid parameter
    - Missing required parameter
    - Invalid rules provided, or missing rules
- 401: Login required
- 403: Permission denied, user does not have permission