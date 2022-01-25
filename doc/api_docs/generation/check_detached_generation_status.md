# GET "/v1/check\_detached\_generation\_status/"

This route returns either a signed S3 URL to the generated file or, if the file is not yet ready or have failed to generate for other reasons, returns a status indicating that. This route is used for file generation **independent** from a submission.

## Sample Request (JSON)
`/v1/check_detached_generation_status/?job_id=1`

## Request Params
- `job_id`: (required, integer) the job_id for the generation. Provided in the response of the call to [generate\_detached\_file](./generate_detached_file.md)

## Response (JSON)

```
{
    "job_id": 1234,
    "status": "finished",
    "file_type": "D1",
    "url": "https://........",
    "size": null,
    "start": "01/01/2016",
    "end": "03/31/2016",
    "message": "",
    "generated_at": "2020-01-15 14:25:40.12345"
}
```

## Response Attributes
- `job_id`: (integer) job ID of the generation job in question
- `status`: (string) indicates the file's status. Possible values are:
    - `finished`: file has been generated and is available for download
    - `waiting`: file has either not started/finished generating or has finished generating but is not yet uploaded to S3
    - `failed`: an error occurred and the file generation or S3 upload failed, the generated file is invalid, or any other error
    - `invalid`: no generation request has ever been made for this submission ID before
- `file_type`: (string) indicates the file that the status data refers to. Possible values are:
    - `D1`
    - `D2`
    - `A`
- `url`: (string) a signed S3 URL from which the generated file can be downloaded. This will be the string `"#"` if the file is not in the `finished` state.
- `size`: (integer) the size of the generated file in bytes
- `start`: (string) the file start date, in `MM/DD/YYYY` format. If not a D1/D2 file, this key will not be returned.
- `end`: (string) the file end date, in `MM/DD/YYYY` format. If not a D1/D2 file, this key will not be returned.
- `message`: (string) a user-readable error message when the file is `failed`, otherwise returns a blank string,
- `generated_at`: (string) the last time (in the `YYYY-MM-DD HH:mm:ss` format) this file was generated in this submission. This does not reflect the time the file itself was generated if it was cached but rather the last time this submission's generation was updated.

#### Errors
Possible HTTP Status Codes:

- 400:
    - Missing `job_id` parameter
    - Submission does not exist
- 401: Login required