# GET "/v1/list\_all\_agencies/"
Returns a list of all CGAC and FREC agencies

## Sample Request
`/v1/list_all_agencies/`
    
## Request Params
N/A

## Response (JSON)
```
{
    "agency_list": [
      {
        "agency_name": "Sample Agency",
        "cgac_code": "000"
      }, ...
    ],
    "shared_agency_list": [
      {
        "agency_name": "Sample FREC Agency",
        "frec_code": "0000"
      }, ...
    ]
}
```

## Response Attributes
- `agency_list`: ([dict]) the list of all CGAC codes and names. Dictionaries in the list contain the following elements:
    -  `agency_name`: (string) the name of the CGAC agency
    -  `cgac_code`: (string) the code of the CGAC agency
-  `shared_agency_list`: ([dict]) the list of all FREC codes and names. Dictionaries in the list contain the following elements:
    -  `agency_name`: (string) the name of the FREC agency
    -  `frec_code`: (string) the code of the FREC agency

## Errors
N/A