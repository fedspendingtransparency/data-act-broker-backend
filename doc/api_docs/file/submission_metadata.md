# GET "/v1/submission\_metadata/"
This endpoint returns metadata for the requested submission.

## Sample Request
`/v1/submission_metadata/?submission_id=123`

## Request Params
- `submission_id`: (required, integer) the ID of the submission to get metadata for

## Response (JSON)
```
{
    "cgac_code": "000",
    "frec_code": null,
    "agency_name": "Agency Name",
    "number_of_errors": 10,
    "number_of_warnings": 20,
    "number_of_rows": 3,
    "total_size": 1800,
    "created_on": "2018-04-16T12:48:09",
    "last_updated": "2018-04-16T18:48:09",
    "last_validated": "2018-04-16T18:48:09",
    "reporting_period": "Q2/2018",
    "reporting_start_date": "01/01/2018",
    "reporting_end_date": "03/31/2018",
    "publish_status": "unpublished",
    "quarterly_submission": false,
    "test_submission": false,
    "published_submission_ids": [],
    "certified": false,
    "certification_deadline": "2020-05-24",
    "fabs_submission": true,
    "fabs_meta": {
        "valid_rows": 1,
        "total_rows": 2,
        "publish_date": null,
        "published_file": null
    }
}
```

## Response Attributes
- `cgac_code`: (string) CGAC of agency (null if FREC agency)
- `frec_code`: (string) FREC of agency (null if CGAC agency)
- `agency_name`: (string) name of the submitting agency
- `number_of_errors`: (integer) total errors in the submission
- `number_of_warnings`: (integer) total warnings in the submission
- `number_of_rows`: (integer) total number of rows in the submission including file headers
- `total_size`: (integer) total size of all files in the submission in bytes
- `created_on`: (string) date submission was created (YYYY-MM-DDTHH:mm:ss)
- `last_updated`: (string) date/time any changes (including validations, etc) were made to the submission (YYYY-MM-DDTHH:mm:ss)
- `last_validated`: (string) date the most recent validations were completed (YYYY-MM-DDTHH:mm:ss)
- `reporting_period`: (string) reporting period of the submission (Q#/YYYY for quarterly submissions, P##/YYYY for monthly, P01-P02/YYYY for period 2)
- `reporting_start_date`: (string) the start date of the reporting period the submission is made for (MM/DD/YYYY format)
- `reporting_end_date`: (string) the end date of the reporting period the submission is made for (MM/DD/YYYY format)
- `publish_status`: (string) whether the submission is published or not. Can contain only the following values:
    - `unpublished`
    - `published`
    - `updated`
    - `publishing`
- `quarterly_submission`: (boolean) whether the submission is quarterly or monthly
- `test_submission`: (boolean) whether the submission is a test submission
- `published_submission_ids`: ([integer]) submission ids published in the same period or quarter by the same agency 
- `certified`: (boolean) whether the submission has been certified or not
- `certification_deadline`: (string) represents the deadline for certification after which a submission is officially "late" to certify.
- `fabs_submission`: (boolean) whether the submission is FABS or DABS (True for FABS)
- `fabs_meta`: (dict) data specific to FABS submissions (null for DABS submissions)
    - `publish_date`: (string) Date/time submission was published (YYYY-MM-DDTHH:mm:ss) (null if unpublished)
    - `published_file`: (string) signed url of the published file (null if unpublished)
    - `total_rows`: (integer) total rows in the submission not including header rows
    - `valid_rows`: (integer) total number of valid, publishable row

## Errors
Possible HTTP Status Codes:

- 400:
    - Missing `submission_id` parameter
    - Submission does not exist
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission