# GET "/v1/check\_generation\_status/"

This route returns either a signed S3 URL to the generated file or, if the file is not yet ready or have failed to generate for other reasons, returns a status indicating that. This route is used for file generation **within** a submission.

## Sample Request
`/v1/check_generation_status/?submission_id=123&file_type=D1`

## Request Params
- `submission_id`: (required, integer) the ID of the current submission
- `file_type`: (required, string) the file type whose status we are checking. Allowable values are:
    - `D1`
    - `D2`
    - `E`
    - `F`

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
    "generated_at": "01/15/2020 14:25:40"
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
    - `E`
    - `F`
- `url`: (string) a signed S3 URL from which the generated file can be downloaded. This will be the string `"#"` if the file is not in the `finished` state.
- `size`: (integer) the size of the generated file in bytes
- `start`: (string) the file start date, in `MM/DD/YYYY` format. If not a D1/D2 file, this key will not be returned.
- `end`: (string) the file end date, in `MM/DD/YYYY` format. If not a D1/D2 file, this key will not be returned.
- `message`: (string) a user-readable error message when the file is `failed`, otherwise returns a blank string
- `generated_at`: (string) the last time (in the `MM/DD/YYYY HH:mm:ss` format) this file was generated in this submission. This does not reflect the time the file itself was generated if it was cached but rather the last time this submission's generation was updated.

## Errors
Possible HTTP Status Codes:

- 400:
    - Missing `submission_id` parameter
    - Submission does not exist
    - Invalid `file_type` parameter
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission