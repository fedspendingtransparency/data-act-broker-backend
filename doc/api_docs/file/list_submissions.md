# POST "/v1/list\_submissions/"
This endpoint lists submissions for all agencies for which the current user is a member of. Optional filters allow for more refined lists.

## Body (JSON)
```
{
    "page": 2,
    "limit": 5,
    "published": "true",
    "sort": "modified",
    "order": "desc",
    "fabs": false,
    "filters": {
        "submission_ids": [123, 456],
        "last_modified_range": {
            "start_date": "01/01/2018",
            "end_date": "01/10/2018"
        },
        "agency_codes": ["123", "4567"],
        "file_names": ["file_a", "test"],
        "user_ids": [1, 2],
        "submission_type": "test"
    }
}
```

## Body Description

- `published`: (required, string) the certification/publish status of the submissions listed. Allowed values are:
    - `true`: only include submissions that have been certified/published
    - `false`: only include submissions that have never been certified/published
    - `mixed`: include both certified/published and non-certified/published submissions
- `page`: (integer) the page of submissions to view (offsets the list by `limit * (page - 1)`). Defaults to `1` if not provided
- `limit`: (integer) the total number of results to see from this request. Defaults to `5` if not provided
- `sort`: (string) what value to sort by. Defaults to `modified` if not provided. Valid values are:
    - `submission_id`: submission id
    - `modified`: last modified date
    - `reporting_start`: reporting start date
    - `reporting_end`: reporting end date
    - `agency`: agency name
    - `submitted_by`: name of user that created the submission
    - `last_pub_or_cert `: most recent date the submission was either published or certified
    - `quarterly_submission`: quarterly submission or not
- `order`: (string) the sort order. Defaults to `desc` if not provided. Valid values are:
    - `desc`
    - `asc`
- `fabs`: (boolean) if the submissions listed should be FABS or DABS (True for FABS). Defaults to `False` if not provided.
- `filters`: (dict) additional filters to narrow the results returned by the endpoint. Possible filters are:
    - `submission_ids`: ([integer]) an array of integers or strings that limits the submission IDs returned to only the values listed in the array.
    - `last_modified_range`: (dict) a start and end date for the last modified date range. If either is not provided, the search is unbounded in that direction (e.g. if the end date is not provided, it finds all submissions that have been modified since the start date)
        - `start_date`: (string) the start date for the last modified date range (inclusive) (MM/DD/YYYY)
        - `end_date`: (string) the end date for the last modified date range (inclusive) (MM/DD/YYYY)
    - `agency_codes`: ([string]) CGAC and FREC codes
    - `file_names`: ([string]) total or partial matches to file names (including timestamps), will match any file name including generated ones
    - `user_ids`: ([string, integer]) limits the list of submissions to only ones created by users within the array.
    - `submission_type`: (string) if present, limits to only showing test or only showing certifiable submissions in the list. Can be used with any other filters or parameters but only unpublished DABS submissions can be tests. Allowable values:
      - `test`
      - `certifiable`

## Response (JSON)
```
{
    "submissions": [
        {
            "submission_id": 1,
            "reporting_start_date": "2016-07-01",
            "reporting_end_date": "2016-09-01",
            "user": {
                "name": "User Name",
                "user_id": 1
            },
            "files": ["file1.csv", "file2.csv"],
            "agency": "Department of the Treasury (TREAS)",
            "status": "validation_successful",
            "last_modified": "2016-08-30 12:59:37.053424",
            "publish_status": "published",
            "test_submission": false,
            "publishing_user": "Certifier",
            "last_pub_or_cert": "2016-08-30 12:53:37.053424",
            "quarterly_submission": true,
            "certified": true,
            "time_period": "FY 16 / Q4"
        },
        {
            "submission_id": 2,
            "reporting_start_date": "2015-07-01",
            "reporting_end_date": "2015-09-01",
            "user": {
                "name": "User2 Name2",
                "user_id": 2
            },
            "files": ["file1.csv", "file2.csv"],
            "agency": "Department of Defense (DOD)",
            "status": "file_errors",
            "last_modified": "2016-08-31 15:59:37.053424",
            "publish_status": "unpublished",
            "test_submission": false,
            "publishing_user": "",
            "last_pub_or_cert": "",
            "quarterly_submission": true,
            "certified": true,
            "time_period": "FY 15 / Q4"
        }
    ],
    "total": 2,
    "min_last_modified": "2016-08-30 12:59:37.053424"
}
```

## Response Attributes

- `total`: (integer) the total submissions that match the provided parameters (including those that didn't fit within the limit)
- `min_last_modified`: (string) the minimum last modified date for submissions with the same type (FABS/DABS) and certify status (certified/published, unpublished, both) as the request (additional filters do not affect this number)
- `submissions`: ([dict]) details about submissions. Contents of each dictionary are:
    - `submission_id`: (integer) ID of the submission
    - `reporting_start_date`: (string) the start date of the submission (`YYYY-MM-DD`)
    - `reporting_end_date`: (string) the end date of the submission (`YYYY-MM-DD`)
    - `user`: (dict) details of the user that created the submission:
        - `name`: (string) the name of the user
        - `user_id`: (integer) the ID of the user
    - `files`: ([string]) file names associated with the submission
    - `agency`: (string) the name of the agency the submission is for
    - `status`: (string) the current status of the submission. Possible values are:
        - `failed`
        - `file_errors`
        - `running`
        - `waiting`
        - `ready`
        - `validation_successful`
        - `validation_successful_warnings`
        - `certified`
        - `validation_errors`
        - `updated`
        - `published`
    - `last_modified`: (string) the last time/date the submission was modified in any way (`YYYY-MM-DD HH:mm:ss`)
    - `publish_status`: (string) the publish status of the submission. Possible values are:
        - `unpublished`
        - `published`
        - `updated`
        - `publishing`
    - `test_submission`: (boolean) whether the submission is a test submission
    - `publishing_user`: (string) the name of the last user to publish the submission
    - `certified`: (boolean) whether the submission has been certified or not
    - `last_pub_or_cert `: (string) the last time/date the submission was published or certified. (`YYYY-MM-DD HH:mm:ss`)
    - `quarterly_submission`: (boolean) whether the submission is quarterly
    - `time_period`: (string) the time frame for the submission

## Errors
Possible HTTP Status Codes:

- 400:
    - Invalid types in a filter
    - invalid values in a filter
    - missing required parameter
- 401: Login required