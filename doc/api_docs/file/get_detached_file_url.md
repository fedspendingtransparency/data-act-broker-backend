# GET "/v1/get\_detached\_file\_url/"
This endpoint returns the signed url for the generated file of the requested job

## Sample Request
`/v1/get_detached_file_url/?job_id=123`

## Request Params
Job IDs is obtained from the `generate_detached_file` endpoint

- `job_id`: (required, integer) the ID of the job to get the file url for

## Response (JSON)
```
{
    "url": "https://......."
}
```

## Response Attributes
- `url`: string, the signed url for the requested file

## Errors
Possible HTTP Status Codes:

- 400:
    - No such job ID
    - The job ID provided is not a detached file generation
    - Missing parameter
- 401: Login required