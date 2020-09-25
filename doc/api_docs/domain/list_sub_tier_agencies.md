# GET "/v1/list\_sub\_tier\_agencies/"
Gets all CGACs/FRECs that the user has submit/certify permissions as well as all sub-tier agencies under them

## Sample Request
`/v1/list_sub_tier_agencies/`

## Request Params
N/A

## Response (JSON)
```
{
    "sub_tier_agency_list": [
      {
        "agency_name": "CGAC Agency: CGAC Sub Tier Agency",
        "agency_code": "0000",
        "priority": "1"
      }, ...
    ]
}
```

## Response Attributes
- `sub_tier_agency_list`: ([dict]) dictionaries hold the following information about each sub tier agency:
    - `agency_name`: (string) the top tier and sub tier agency names formatted `Top Tier: Sub Tier`
    - `agency_code`: (string) the code associated with the sub tier agency
    - `priority`: (integer) whether the sub tier is used as a stand-in for the top tier agency or if it's truly a sub tier agency. Possible values:
        - `1`: the top tier agency's sub tier
        - `2`: a true sub tier agency that is contained under the indicated top tier agency

## Errors
N/A