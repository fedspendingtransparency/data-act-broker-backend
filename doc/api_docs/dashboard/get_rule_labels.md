# POST "/v1/get\_rule\_labels/"
Gets a list of error/warning lables that pertain to the filters provided.

## Body (JSON)
```
    {
        "files": ["A", "B", "cross-AB"],
        "fabs": false,
        "error_level": "warning"
    }
```

## Body Description
- `files`: (required, array) Lists the files to get rule labels for. If an empty list is provided, all rule labels that match the remaining filters will be returned. If retrieving rules for a FABS submission, send an empty files list. Capitalization matters. Allowed values are:
    - `A`: Appropriations
    - `B`: Program Activity
    - `C`: Award Financial
    - `cross-AB`: cross-file between Appropriations and Program Activity
    - `cross-BC`: cross-file between Program Activity and Award Financial
    - `cross-CD1`: cross-file between Award Financial and Award Procurement
    - `cross-CD2`: cross-file between Award Financial and Award Financial Assistance
- `fabs`: (boolean) Determines whether labels being gathered are for FABS or DABS rules. True if FABS. Defaults to false if not provided
- `error_level`: (string) Determines whether to provide error or warning rule labels. Defaults to `warning` if not provided. Allowed values:
    - `error`
    - `warning`
    - `mixed`

## Response (JSON)
```
{
    "labels": ["A3", "A11"]
}
```

## Response Attributes
- `labels`: (array) The list of rule labels (strings) that correspond to the values provided in the request

## Errors
Possible HTTP Status Codes:

- 400:
    - Files provided for FABS rule list
    - Invalid file type provided
    - Invalid parameter type provided