# GET "/v1/list\_data\_sources/"
Returns a list of all external data sources and the latest load dates

## Sample Request
`/v1/list_data_sources/`
    
## Request Params
N/A

## Response (JSON)
```
{
    "usps_download": "10/01/2021 08:52:12",
    "program_activity_upload": "09/31/2020 17:01:11",
    ...
}
```

## Response Attributes
Each key is the name of an external data source. The value associated with the key is when the external load was last successfully completed with new data inserted into the database. Possible values are:

- `usps_download`
- `program_activity_upload`

## Errors
N/A