# POST "/v1/generate\_file/"
This route sends a request to the backend to utilize the relevant external APIs and generate the relevant file for the metadata that is submitted. This route is used for file generation **within** a submission.

## Body (JSON)
```
{
    "submission_id": 123,
    "file_type": "D1",
    "start": "01/01/2016",
    "end": "03/31/2016",
    "agency_type": "awarding",
    "file_format": "csv"
}
```

## Body Description

- `submission_id`: (required, integer) the ID of the submission to generate a file for
- `file_type`: (required, string) the file type to generate. Allowable values are:
    - `D1`: generate a D1 file
    - `D2`: generate a D2 file
    - `E`: generate an E file
    - `F`: generate an F file
- `start`: (string) the start date of the requested date range, in `MM/DD/YYYY` string format, should not be used for E/F generation
- `end`: (string) the end date of the requested date range, in `MM/DD/YYYY` string format, should not be used for E/F generation
- `agency_type`: (string) indicates if the file generated should be based on awarding or funding agency. Defaults to `awarding` if not provided. Only used in D1/D2 generation. Only allowed values are:
    - `awarding`
    - `funding`
- `file_format`: (string) indicates if the file generated should be a comma delimited csv or a pipe delimited txt. Defaults to `csv` if not provided. Only used in D1/D2 generation. Only allowed values are:
    - `csv`
    - `txt`

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
    "message": ""
}
```

## Response Attributes
If the file has not finished generating when this returns, further checks for whether the generation is complete or not should be done using `check_generation_status`.

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

## Errors
Possible HTTP Status Codes not covered by `check_generation_status` documentation:

- 400:
    - Start and end date not provided for D1/D2 generation
    - Start and end date not formatted properly
    - Missing `submission_id` parameter
    - Submission does not exist
    - Invalid `file_type` parameter
- 401: Login required
- 403: Permission denied, user does not have permission to edit this submission