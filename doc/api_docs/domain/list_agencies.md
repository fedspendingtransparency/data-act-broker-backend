# GET "/v1/list\_agencies/"
Gets a list of all CGACs/FRECs that the user has permissions for.

## Sample Request
`/v1/list_agencies/`

## Request Params
- `perm_level`: (string) indicates the permission level to filter on. Defaults to reader if not provided. Allowable values are:
    - `reader`: include all agencies with affiliations
    - `writer`: include all agencies with writer/editfabs affiliation or above
    - `submitter`: include all agencies with submitter/fabs affiliation
- `perm_type`: (string) indicates the permission type to filter on. Defaults to mixed if not provided Allowable values are:
    - `dabs`: include all agencies with dabs affiliations
    - `fabs`: include all agencies with fabs affiliations
    - `mixed`: include all agencies with dabs or fabs affiliation

## Response (JSON)
```
{
    "cgac_agency_list": [
        {
            "agency_name": "Sample Agency",
            "cgac_code": "000"
        },
        {
            "agency_name": "Sample Agency 2",
            "cgac_code": "998"
        }
    ],
    "frec_agency_list": [
        {
            "agency_name": "Sample FREC Agency",
            "frec_code": "0000"
        }
    ]
}
```

## Response Attributes
- `cgac_agency_list`: (list[dict]) A list of all cgac agencies (cgac code and agency name) the user has permissions to access.
- `frec_agency_list `: (list[dict]) A list of all frec agencies (frec code and agency name) the user has permissions to access.

## Errors
Possible HTTP Status Codes:

- 401: Login required