# POST "/v1/generate\_detached\_file/"

This route sends a request to the backend to generate the relevant file for the metadata that is submitted. This route is used for file generation **independent** from a submission. For more details on how A files are generated, see the [FileLogic.md](../../FileLogic.md) file.

## Body (JSON)

```
{
    "file_type": "D1",
    "cgac_code": "020",
    "start": "01/01/2016",
    "end": "03/31/2016",
    "year": 2017,
    "period": 3,
    "agency_type": "awarding",
    "file_format": "csv",
    "element_numbers": True
}
```

## Body Description

- `file_type`: (required, string) indicates the file type to generate. Allowable values are:
    - `D1`: generate a D1 file
    - `D2`: generate a D2 file
    - `A`: generate an A file
- `cgac_code`: (string) The cgac of the agency for which to generate the files. Required if `frec_code` is not provided.
- `frec_code`: (string) The frec of the agency for which to generate the files. Required if `cgac_code` is not provided.
- `start`: (string) The start date of the requested date range, in `MM/DD/YYYY` string format. Required for D file generation, ignored in A file generation.
- `end`: (string) The end date of the requested date range, in `MM/DD/YYYY` string format. Required for D file generation, ignored in A file generation.
- `year`: (integer) Indicates the year for which to generate an A file. Required for A file generation, ignored in D file generation.
- `period`: (integer) Indicates the period for which to generate an A file. Required for A file generation, ignored in D file generation.
    - Allowed values: 2-12
    - 2 indicates November of the previous year, 12 indicates September of the selected year
- `agency_type`: (string) Indicates if the file generated should be based on awarding or funding agency. Used only in D file generation. Defaults to `awarding` if not provided. Only allowed values are:
    - `awarding`
    - `funding`
- `file_format`: (string) Indicates if the file generated should be a comma delimited csv or a pipe delimited txt. Used only in D file generation. Defaults to `csv` if not provided. Only allowed values are:
    - `csv`
    - `txt`
- `element_numbers`: (boolean) Indicates whether to include FPDS element numbers in the D1 headers. Used only in D1 file generation, ignored in all others. Defaults to `False`

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
If the file has not finished generating when this returns, further checks for whether the generation is complete or not should be done using [check\_detached\_generation\_status](./check_detached_generation_status.md).

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
- `message`: (string) a user-readable error message when the file is `failed`, otherwise returns a blank string

## Errors
Possible HTTP Status Codes not covered by `check_generation_status` documentation:

- 400:
    - Invalid `file_type` parameter
    - Missing cgac or frec code
    - Missing start/end date OR period/year (depending on generation type)
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission