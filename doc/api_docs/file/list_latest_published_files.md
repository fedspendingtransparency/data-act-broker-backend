# GET "/v1/list\_latest\_published\_files/"
Allows users to find available the latest published files by all agencies, available to all Broker users.    

## Sample Request
`/v1/list_latest_published_files?type=fabs&agency=012&year=2017&period=12`

## Request Params

- `type`: (required, string) the type of requested submission file. Acceptable values:
    * `dabs`
    * `fabs`
- `agency`: (integer) the agency of requested submission file
- `year`: (integer) the fiscal year of requested submission file
- `period`: (integer) the fiscal period of requested submission file

## Response (JSON)
Given the nature of the endpoint, each response will be different in content, though the same in formatting.

If `agency` is not provided, results will be agencies:
```
[
    {
        "id": "012",
        "label": "012 - Department of Agriculture (USDA)"
    },
    {
        "id": "7801",
        "label": "7801 - Farm Credit Administration (FCA)"
    }
]
```
If `year` is not provided, results will be years:
```
[
    {
        "id": 2017,
        "label": "2017"
    },
    {
        "id": 2019,
        "label": "2019"
    },
    {
        "id": 2021,
        "label": "2021"
    }
]
```
If `period` is not provided, results will be periods:
```
[
    {
        "id": 12,
        "label": "P12/Q4"
    }
]
```
If all are provided, results will be the files:
```
[
    {
        "id": 189,
        "label": "1631034602_submission_41786_published_fabs.csv",
        "filetype": "FABS",
        "submission_id": 33
    }
]
```

## Response Attributes
- list of result objects, each with the following attributes
    - `id`: (int) the unique identifier for the result
        - agency code for agencies
        - year for years
        - fiscal year period for periods
        - `published_files_history_id` for files
    - `label`: (string) the proper display name for the result
    - `filetype`: (string) the file type letter/name the file represents (only for the deepest level)
    - `submission_id`: (int) the submission_id of the file's submission (only for the deepest level)

## Errors
Possible HTTP Status Codes:

- 400:
    - Invalid `type` parameter
    - Missing `type` parameter
    - User isn't logged in
