# The DATA Act Broker Application Programming Interface (API)

The DATA Act Broker API powers the DATA Act's data submission process.

## Background

The U.S. Department of the Treasury is building a suite of open-source tools to help federal agencies comply with the [DATA Act](http://fedspendingtransparency.github.io/about/ "Federal Spending Transparency Background") and to deliver the resulting standardized federal spending information back to agencies and to the public.

For more information about the DATA Act Broker codebase, please visit this repository's [main README](../README.md "DATA Act Broker Backend README").

## Broker API Project Layout

The Broker API has two major directories: scripts and handlers.

```
dataactbroker/
├── scripts/        (Install and setup scripts)
└── handlers/       (Route handlers)
```

### Scripts
The `/dataactbroker/scripts` folder contains the install scripts needed to setup the broker API for a local install. For complete instructions on running your own copy of the API and other DATA Act broker components, please refer to the [documentation in the DATA Act core responsitory](https://github.com/fedspendingtransparency/data-act-broker-backend/blob/master/doc/INSTALL.md "DATA Act broker installation guide").

### Handlers
The `dataactbroker/handlers` folder contains the logic to handle requests that are dispatched from the `loginRoutes.py`, `fileRoutes.py`, and 'userRoutes.py' files. Routes defined in these files may include the `@requires_login` and `@requires_submission_perms` tags to the route definition. This tag adds a wrapper that checks if there exists a session for the current user and if the user is logged in, as well as checking the user's permissions to determine if the user has access to this route. If user is not logged in to the system or does not have access to the route, a 401 HTTP error will be returned. This tags are defined in `dataactbroker/permissions.py`.

`accountHandler.py` contains the functions to check logins and to log users out.

`fileHandler.py` contains functions for managing user file interaction. It creates all of the jobs that are part of the user submission and has query methods to get the status of a submission. In addition, this class creates downloadable links to error reports created by the DATA Act Validator.

## DATA Act Broker Route Documentation

All routes that require a login should now be passed a header "x-session-id".  The value for this header should be taken
from the login route response header "x-session-id".

### Status Codes
In general, status codes returned are as follows:

* 200 if successful
* 400 if the request is malformed
* 401 if the username or password are incorrect, or the session has expired
* 500 for server-side errors

### GET "/"
This route confirms that the broker is running

Example input:

None

Example output:

"Broker is running"

### User Routes

#### POST "/v1/max_login/"
This route sends a request to the backend with the ticket obtained from the MAX login endpoint in order to verify authentication and access to the Data Broker.

#### Body (JSON)

```
{
    "ticket": ST-123456-abcdefghijklmnopqrst-login.max.gov,
    "service": http%3A%2F%2Furl.encoded.requesting.url%2F
}
```

#### Body Description

* `ticket` - ticket string received from MAX from initial login request (pending validation)
* `service` - URL encoded string that is the source of the initial login request

#### Response (JSON)
Response will be somewhat similar to the original `/login` endpoint. More data will be added to the response depending on what we get back from MAX upon validating the ticket.

```
{
    "message": "Login successful",
    "user_id": 42,
    "name": "John",
    "title":"Developer",
    "agency": "Department of Labor",
    "permission" : 1
}
```

#### POST "/v1/login/"
This route checks the username and password against a credentials file. Accepts input as json or form-urlencoded, with keys "username" and "password". See `current_user` docs for details.

Example input:

```json
{
    "username": "user",
    "password": "pass"
}
```

Example output:

```json
{
    "message": "Login successful",
    "user_id": 42,
    "name": "Jill",
    "title":"Developer",
    "skip_guide": False,
    "website_admin": False,
    "affiliations": [
        {"agency_name": "Department of Labor", "permission": "writer"}
    ]
}
```

#### POST "/v1/logout/"
Logs the current user out, only the login route will be accessible until the next login.  If not logged in, just stays logged out. Returns 200 in both cases.

Example input:

None

Example output:

```json
{
    "message": "Logout successful"
}
```

#### GET "/v1/session/"
Checks that the session is still valid. Returns 200, and JSON with key "status" containing True if the session exists, and False if it doesn't.

Example input:

None

Example output:

```json
{
    "status": "True"
}
```

#### GET "/v1/current_user/"
Gets the information of the current that is login to the system.

Example input:

None

Example output:

```json
{
    "user_id": 42,
    "name": "John",
    "title":"Developer",
    "skip_guide": False,
    "website_admin": False,
    "affiliations": [
        {"agency_name": "Department of Labor", "permission": "writer"}
    ]
}
}
```

* `skip_guide` indicates whether or not the user has requested to skip introductory materials.
* `website_admin` describes a super-user status.
* `affiliations` is a list of objects indicating which agencies this user is a part of and what permissions they have at that agency.


#### POST "/v1/set_skip_guide/"
Sets skip_guide parameter for current user, which controls whether the submission guide should be displayed.  A call to this route should have JSON or form-urlencoded with key "skip_guide", value should be either true or false.

Example input:

```json
{
   "skip_guide": True
}
```

Example output:

```json
{
  "message": "skip_guide set successfully",
  "skip_guide": True
}
```

### File Routes

#### GET "/"
This route confirms that the broker is running

Example input: None
Example output: "Broker is running"

#### GET "/\<filename\>"
This path will return files located in the local folder. This path is only accessible for local installs due
to security reasons.

Example Route `/Users/serverdata/test.csv`  for example will return the `test.csv` if the local folder points
to `/Users/serverdata`.

#### POST "/v1/local_upload/"
Input for this route should be a post form with the key of `file` where the uploaded file is located. This route **only** will
return a success for local installs for security reasons. Upon successful upload, file path will be returned.

Example Output:
```json
{
   "path": "/User/localuser/server/1234_filename.csv"
}
```

#### POST "/v1/submit_files/"
This route is used to retrieve S3 URLs to upload files. Data should be JSON with keys: ["appropriations", "award_financial", "award", "program_activity"], each with a filename as a value, and submission metadata keys: ["agency_name","reporting_period_start_date","reporting_period_end_date","is_quarter","existing_submission_id"].  If an existing submission ID is provided, all other keys are optional and any data provided will be used to correct information in the existing submission.

This route will also add jobs to the job tracker DB and return conflict free S3 URLs for uploading. Each key put in the request comes back with an url_key containing the S3 URL and a key_id containing the job id. A returning submission_id will also exist which acts as identifier for the submission.

A credentials object is also part of the returning request. This object provides temporarily access to upload S3 Files using an AWS SDK. It contains the following: SecretAccessKey, SessionToken, Expiration, and AccessKeyId.
It is important to note that the role used to create the credentials should be limited to just S3 access.

When upload is complete, the finalize_submission route should be called with the job_id.

Example input:

```json
{
  "appropriations":"appropriations.csv",
  "award_financial":"award_financial.csv",
  "award":"award.csv",
  "program_activity":"program_activity.csv",
  "agency_name":"Name of the agency",
  "reporting_period_start_date":"03/31/2016",
  "reporting_period_end_date":"03/31/2016",
  "is_quarter":False,
  "existing_submission_id: 7 (leave out if not correcting an existing submission)
}
```

Example output:

```json
{
  "submission_id": 12345,

  "bucket_name": "S3-bucket",

  "award_id": 100,
  "award_key": "2/1453474323_awards.csv",

  "appropriations_id": 101,
  "appropriations_key": "2/1453474324_appropriations.csv",

  "award_financial_id": 102,
  "award_financial_key": "2/1453474327_award_financial.csv",

  "program_activity_id": 103,
  "program_activity_key": "2/1453474333_program_activity.csv",

  "credentials": {
    "SecretAccessKey": "ABCDEFG",
    "SessionToken": "ABCDEFG",
    "Expiration": "2016-01-22T15:25:23Z",
    "AccessKeyId": "ABCDEFG"
  }
}
```

#### POST "/v1/finalize_job/"
A call to this route should have JSON or form-urlencoded with a key of "upload_id" and value of the job id received from the submit_files route. This will change the status of the upload job to finished so that dependent jobs can be started.

Example input:

```json
{
  "upload_id":3011
}
```

Example output:

```json
{
  "success": true
}
```

#### POST "/v1/submission_error_reports/"
A call to this route should have JSON or form-urlencoded with a key of "submission_id" and value of the submission id received from the submit_files route.  The response object will be JSON with keys of "job_X_error_url" for each job X that is part of the submission, and the value will be the signed URL of the error report on S3. Note that for failed jobs (i.e. file-level errors), no error reports will be created.

Example input:

```json
{
   "submission_id":1610
}
```

Example output:

```json
{
  "job_3012_error_url": "https...",
  "job_3006_error_url": "https...",
  "job_3010_error_url": "https...",
  "job_3008_error_url": "https...",
  "cross_appropriations-program_activity": "https..."
}
```

#### POST "/v1/submission_warning_reports/"
A call to this route should have JSON or form-urlencoded with a key of "submission_id" and value of the submission id received from the submit_files route.  The response object will be JSON with keys of "job_X_warning_url" for each job X that is part of the submission, and the value will be the signed URL of the error report on S3. Note that for failed jobs (i.e. file-level errors), no error reports will be created.

Example input:

```json
{
   "submission_id":1610
}
```

Example output:

```json
{
  "job_3012_warning_url": "https...",
  "job_3006_warning_url": "https...",
  "job_3010_warning_url": "https...",
  "job_3008_warning_url": "https...",
  "cross_warning_appropriations-program_activity": "https..."
}
```

#### POST "/v1/check_status/"
A call to this route will provide status information on all jobs associated with the specified submission.
The request should have JSON or form-urlencoded with a key "submission_id".  The response will contain a list of
status objects for each job under the key "jobs", and other submission-level data.  In error data,
"original_label" will only be populated when "error_name" is "rule_failed".  List of keys in response:
- jobs: Holds data for each job in submission, each job is a dict with:
    * job_id: Internal ID we have assigned to this job
    * job_status: Values are:
        - waiting: Job has prerequisites that are not complete, such as a file upload
        - ready: Job is ready to be started but has not yet reached the validator
        - running: Job was started on the validator, but has not yet completed or errored
        - finished: Job completed with no errors (may still have warnings)
        - invalid: Job completed with errors
        - failed: There was an unexpected error in the validator, job should be restarted.
    * file_type: Which type of file this job is for, will be blank for cross-file jobs.  Values are 'appropriations', 'program_activity', 'award_financial', and 'award'.
    * job_type: Values are:
        - file_upload: This job is a placeholder for the frontend's upload of the file
        - csv_record_validation: File level validation job
        - validation: Cross file validation job
    * filename: Original filename as submitted by user
    * file_status:  Represents status of file being validated, values are:
        - complete: Validation has finished, errors or warnings may have been reported
        - header_error: Validation stopped after finding header errors
        - unknown_error:  Something unexpected went wrong
        - single_row_error:  File has at most one row, valid files must have a header row and at least one row with data
        - job_error: There was a problem with the job sent to the validator, it was either the wrong type or not ready to run
        - incomplete: File is still being validated
    * missing_headers: List of headers that should be present but are not
    * duplicated_headers: List of headers that were included more than once.  If file_status is 'header_error', at least one of 'missing_headers' and 'duplicated_headers' will be non-empty.
    * file_size: Size of submitted file in bytes
    * number_of_rows: Number of rows in submitted file, will only be provided if validation completed
    * error_type: Values are:
        - header_errors: Header errors were found, validation was not completed
        - row_errors: Validation completed with row errors found
        - none: No errors were found, warnings may still be present
    * error_data: Holds a list of errors, each error is a dict with keys:
        - field_name: What field the error occurred on
        - error_name: Type of error that occurred, values are:
            * type_error: Value was of the wrong type
            * required_error: A required value was missing
            * read_error: Could not parse this value from the file
            * write_error: This was a problem writing the value to the staging table
            * rule_failed: A rule failed on this value
            * length_error: Value was too long for this field
        - error_description: Description of the error
        - occurrences: Number of times this error occurred for this field throughout the file.  See the error report for list of rows.
        - rule_failed: Text of the rule that failed
        - original_label: Label of the rule as it appears in the practices and procedures document
    * warning_data: Holds the same information as error_data, but for warnings instead of errors
- agency_name: Which agency this submission is attached to
- cgac_code: The CGAC code associated with that agency
- reporting_period_start_date: Specified by user at time of submission
- reporting_period_end_date: Specified by user at time of submission
- number_of_errors: Total number of errors that have occurred throughout the submission
- number_of_rows: Total number of rows in the submission
- created_on: Date the submission was originally created
- last_updated: Date + time of last modification to this submission
- last_validated: Earliest date of last validation of all jobs
- revalidation_threshold: Earliest date all jobs must be validated after in order to certify/publish
- publish_status: Publish status of the submission (unpublished/published/updated)
- quarterly_submission: True if the submission is quarterly, false otherwise


Example input:

```json
{
  "submission_id":1610
}
```

Example output:

```json
{
  "jobs": [
    {
      "job_id": 3005,
      "job_status": "invalid",
      "file_type": "appropriations",
      "job_type": "file_upload",
      "filename": "approp.csv",
      "file_status" : "header_error",
      "missing_headers": ["header_1", "header_2"],
      "duplicated_headers": ["header_3", "header_4"],
      "file_size": 4508,
      "number_of_rows": 500,
      "error_type": "header_error",
      "error_data": [],
      "warning_data": []
    }, {
      "job_id": 3006,
      "job_status": "finished",
      "file_type": "appropriations",
      "job_type": "file_upload",
      "filename": "approp.csv",
      "file_status" : "complete",
      "missing_headers": [],
      "duplicated_headers": [],
      "file_size": 4508,
      "number_of_rows": 500,
      "error_type": "record_level_error",
      "error_data":  [
        {
          "field_name": "allocationtransferagencyid",
          "error_name": "type_error",
          "error_description": "The value provided was of the wrong type",
          "occurrences": 27,
          "rule_failed": "",
          "original_label":""
        }, {
          "field_name": "status_of_budgetary_resour_cpe, budget_authority_available_cpe",
          "error_name": "rule_failed",
          "error_description": "StatusOfBudgetaryResourcesTotal_CPE = BudgetAuthorityAvailableAmountTotal_CPE",
          "occurrences": 27,
          "rule_failed": "StatusOfBudgetaryResourcesTotal_CPE = BudgetAuthorityAvailableAmountTotal_CPE",
          "original_label":"A24"
        }
      ],
      "warning_data": [
        {
          "field_name": "allocationtransferagencyid",
          "error_name": "rule_failed",
          "error_description": "BorrowingAuthorityAmountTotal_CPE= CPE aggregate value for GTAS SF 133 line #1340 + #1440",
          "occurrences": 27,
          "rule_failed": "BorrowingAuthorityAmountTotal_CPE= CPE aggregate value for GTAS SF 133 line #1340 + #1440",
          "original_label":"A10"
        }, {
          "field_name": "other_budgetary_resources_cpe, borrowing_authority_amount_cpe, contract_authority_amount_cpe, spending_authority_from_of_cpe",
          "error_name": "rule_failed",
          "error_description": "OtherBudgetaryResourcesAmount_CPE must be provided if TAS has borrowing, contract and/or spending authority provided in File A. If not applicable, leave blank.",
          "occurrences": 27,
          "rule_failed": "OtherBudgetaryResourcesAmount_CPE must be provided if TAS has borrowing, contract and/or spending authority provided in File A. If not applicable, leave blank.",
          "original_label":"A28"
        }
      ]
    }, {
      "job_status": "finished",
      "error_data": [
        {
          "error_description": "A rule failed for this value",
          "error_name": "rule_failed",
          "field_name": "award_financial",
          "occurrences": "11",
          "rule_failed": "Must have either a piid, fain, or uri",
          "original_label":""
        }, {
          "error_description": "A rule failed for this value",
          "error_name": "rule_failed",
          "field_name": "award",
          "occurrences": "10",
          "rule_failed": "Must have either a piid, fain, or uri",
          "original_label":""
        }
      ],
      "warning_data": [],
      "missing_headers": [],
      "job_id": 599,
      "file_type": "",
      "error_type": "none",
      "job_type": "validation",
      "filename": null,
      "file_status": "complete",
      "number_of_rows": null,
      "file_size": null,
      "duplicated_headers": []
    }
  ],
  "agency_name": "Name of the agency",
  "cgac_code": "012",
  "reporting_period_start_date": "03/31/2016",
  "reporting_period_end_date": "03/31/2016",
  "number_of_errors": 54,
  "number_of_rows": 446,
  "created_on": "04/01/2016",
  "last_updated": "2016-04-01T09:10:11",
  "last_validated": "03/15/2017",
  "revalidation_threshold": "02/02/2017",
  "publish_status": "unpublished",
  "quarterly_submission": True
}
```

#### GET "/v1/get_protected_files/"
This route returns a signed S3 URL for all files available to download on the help page.

Example output:

```json
{
    "urls": {
            "AgencyLabel_to_TerseLabel.xslx": "https://prod-data-act-submission.s3-us-gov-west-1.amazonaws.com:443/rss/AgencyLabel_to_TerseLabel.xslx?Signature=abcdefg......",
            "File2.extension": "https://......"
    }
}
```

Example output if there are no files available:

```json
{
    "urls": {}
}
```

#### POST "/v1/get_obligations/"
Get total obligations and specific obligations. Calls to this route should include the key "submission_id" to specify which submission we are calculating obligations from.

##### Body (JSON)

```
{
    "submission_id": 123,
}
```

##### Response (JSON)

```
{
  "total_obligations": 75000.01,
  "total_procurement_obligations": 32500.01,
  "total_assistance_obligations": 42500
}
```


#### GET "/v1/submission/\<int:submission_id\>/narrative"
Retrieve existing submission narratives (explanations/notes for particular
files). Submission id should be the integer id associated with the submission
in question. Users must have appropriate permissions to access these
narratives (write access for the agency of the submission or SYS).

##### Response (JSON)

```
{
  "A": "Text of A's narrative",
  "B": "These will be empty if no notes are present",
  "C": "",
  "D1": "",
  "D2": "",
  "E": "",
  "F": "",
}
```

#### POST "/v1/submission/\<int:submission_id\>/narrative"
Set the file narratives for a given submission. The input should mirror the
above output, i.e. an object keyed by file types mapping to strings. Keys may
be absent. Unexpected keys will be ignored. Users must have appropriate
permissions (write access for the agency of the submission or SYS).

##### Body (JSON)

```
{
  "A": "Some new text"
  "C": "We didn't include B",
  "D1": "",
  "D2": "",
  "F": "Or E, for some reason",
}
```

##### Response (JSON)

```
{}
```

#### POST "/v1/submission/\<int:submission_id\>/report_url"
This route requests the URL associated with a particular type of submission report. The provided URL will expire after roughly half an hour.

##### Body (JSON)

```
{
    "warning": True,
    "file_type": "appropriations",
    "cross_type": "award_financial"
}
```

##### Response (JSON)

```
{
  "url": "https://........"
}
```

##### Request Params
  * warning - Whether or not the requested report is a warning (or error)
    report. Defaults to False if this parameter isn't present.
  * file_type - One of 'appropriations', 'program_activity',
    'award_financial', 'award', 'award_procurement', 'awardee_attributes'
    or 'sub_award'. Designates the type of report you're seeking.
  * cross_type - If present, indicates that we're looking for a
    cross-validation report between `file_type` and this parameter. It accepts the
    same values as `file_type`

##### Response
File download or redirect to signed URL

#### POST "/v1/submit_detached_file"

This route sends a request to the backend with ID of the FABS submission we're submitting in order to process it.

##### Body (JSON)

```
{
    "submission_id": 7
}
```

##### Body Description

* `submission_id` - **required** - ID of the submission to process

##### Response (JSON)
Successful response will contain the submission_id.

```
{
    "submission_id": 7
}
```

Invalid submission_ids (nonexistant or not FABS submissions) and submissions that have already been published will return a 400 error.

Other errors will be 500 errors

#### POST "/v1/delete_submission"

This route deletes all data related to the specified `submission_id`. A submission that has ever been certified/published (has a status of "published" or "updated") cannot be deleted.

##### Body (JSON)

```
{
  "submission_id": 1
}
```

##### Body Description

* `submission_id` - **required** - an integer corresponding to the ID of the submission that is to be deleted.

##### Response (JSON)

```
{
  "message": "Success"
}
```
* `message` - A message indicating whether or not the action was successful. Any message other than "Success" indicates a failure.

#### POST "/v1/certify_submission"

This route certifies the specified submission, if possible. If a submission has critical errors, it cannot be certified. Submission files are copied to a certified bucket on aws if it is a non-local environment.

##### Body (JSON)

```
{
  "submission_id": 1
}
```

##### Body Description

* `submission_id` - **required** - an integer corresponding to the ID of the submission that is to be certified.

##### Response (JSON)

```
{
  "message": "Success"
}
```
* `message` - A message indicating whether or not the action was successful. Any message other than "Success" indicates a failure.

#### POST "/v1/restart_validation"

This route alters a submission's jobs' statuses and then restarts all validations for the specified submission.

##### Body (JSON)

```
{
  "submission_id": 1
}
```

##### Body Description

* `submission_id` - **required** - an integer corresponding to the ID of the submission for which the validations should be restarted.

##### Response (JSON)

```
{
  "message": "Success"
}
```
* `message` - A message indicating whether or not the action was successful. Any message other than "Success" indicates a failure.

## File Generation Routes

#### GET "/v1/list_submissions/"
List submissions for all agencies for which the current user is a member of. Optional query parameters are `?page=[page #]&limit=[limit #]&certified=[true|false]` which correspond to the current page number and how many submissions to return per page (limit). If the query parameters are not present, the default is `page=1`, `limit=5` and if `certified` is not provided, all submissions will be returned containing a mix of the two.

##### Example input:

`/v1/list_submissions?page=1&limit=2

##### Example output:

"total" is the total number of submissions available for that user.

```json
{
  "submissions": [
    {
      "reporting_end_date": "2016-09-01",
      "submission_id": 1,
      "reporting_start_date": "2016-07-01",
      "user": {
        "name": "User Name",
        "user_id": 1
      },
      "agency": "Department of the Treasury (TREAS)"
      "status": "validation_successful" (will be undergoing changes),
      "size": 0,
      "errors": 0,
      "last_modified": "2016-08-31 12:59:37.053424",
      "publish_status": "published",
      "certifying_user": "Certifier",
      "certified_on": "2016-08-30 12:53:37.053424"
    },
    {
      "reporting_end_date": "2015-09-01",
      "submission_id": 2,
      "reporting_start_date": "2015-07-01",
      "user": {
        "name": "User2 Name2",
        "user_id": 2
      },
      "agency": "Department of Defense (DOD)"
      "status": "file_errors" (will be undergoing changes),
      "size": 34482,
      "errors": 582,
      "last_modified": "2016-08-31 15:59:37.053424",
      "publish_status": "unpublished",
      "certifying_user": "",
      "certified_on": ""
    }
  ],
  "total": 2
}
```

#### GET "/v1/list_agencies/"
Gets all CGACS that the user has submit/certify permissions

Example input:

None

Example output:

```json
{
    "cgac_agency_list": [
      {
        "agency_name": "Sample Agency",
        "cgac_code": "000"
      }, ...
    ]
}
```

#### GET "/v1/list_all_agencies/"
Gets all CGACS

Example input:

None

Example output:

```json
{
    "cgac_agency_list": [
      {
        "agency_name": "Sample Agency",
        "cgac_code": "000"
      }, ...
    ]
}
```

#### GET "/v1/list_sub_tier_agencies/"
Gets all CGACS that the user has submit/certify permissions as well as all sub-tier agencies under said cgacs

Example input:

None

Example output:

```json
{
    "sub_tier_agency_list": [
      {
        "agency_name": "Sample Agency",
        "agency_code": "000",
	"priority": "0"
      }, ...
    ]
}
```

## Generate Files
**Route:** `/v1/generate_file`

**Method:** `POST`

This route sends a request to the backend to utilize the relevant external APIs and generate the relevant file for the metadata that is submitted.

**Deprecation Notice:** This route replaces `/v1/generate_d1_file` and `/v1/generate_d2_file`.

### Body (JSON)

```
{
    "submission_id": 123,
    "file_type": "D1"
    "start": "01/01/2016",
    "end": "03/31/2016"
}
```

### Body Description

* `submission_id` - **required** - an integer representing the ID of the current submission
* `file_type` - **required** - a string indicating the file type to generate. Allowable values are:
	* `D1` - generate a D1 file
	* `D2` - generate a D2 file
	* `E` - generate a E file
	* `F` - generate a F file
* `start` - **required for D1/D2 only** - the start date of the requested date range, in `MM/DD/YYYY` string format
* `end` - **required for D1/D2 only** - the end date of the requested date range, in `MM/DD/YYYY` string format

### Response (JSON)
Response will be the same format as those which are returned in the `/v1/check_generation_status` endpoint


## File Status
**Route:** `/v1/check_generation_status`

**Method:** `POST`

This route returns either a signed S3 URL to the generated file or, if the file is not yet ready or have failed to generate for other reasons, returns a status indicating that.

**Deprecation Notice:** This route replaces `/v1/check_d1_file` and `/v1/check_d2_file`.

### Body (JSON)

```
{
    "submission_id": 123,
    "file_type": "D1",
    "size": 123
}
```

### Body Description

* `submission_id` - An integer representing the ID of the current submission
* `file_type` - **required** - a string indicating the file type whose status we are checking. Allowable values are:
	* `D1` - generate a D1 file
	* `D2` - generate a D2 file
	* `E` - generate a E file
	* `F` - generate a F file


### Response (JSON)

*State:* The file has successfully generated

```
{
	"status": "finished",
	"file_type": "D1",
	"url": "https://........",
	"start": "01/01/2016",
	"end": "03/31/2016",
	"message": ""
}
```

*State:* The file is not yet ready

```
{
	"status": "waiting",
	"file_type": "D1",
	"url": "",
    "start": "01/01/2016",
    "end": "03/31/2016",
    "message": ""
}
```

*State:* File generation has failed

```
{
	"status": "failed",
	"file_type": "D1",
	"url": "",
    "start": "01/01/2016",
    "end": "03/31/2016",
	"message": "The server could not reach the Federal Procurement Data System. Try again later."
}
```

*State:* No file generation request has been made for this submission ID before

```
{
	"status": "invalid",
	"file_type": "D1",
	"url": "",
	"start": "",
	"end": "",
	"message": ""
}
```


### Response Description

The response is an object that represents that file's state.

* `status` - a string constant indicating the file's status.
	* Possible values are:
		* `finished` - file has been generated and is available for download
		* `waiting` - file has either not started/finished generating or has finished generating but is not yet uploaded to S3
		* `failed` - an error occurred and the file generation or S3 upload failed, the generated file is invalid, or any other error
		* `invalid` - no generation request has ever been made for this submission ID before

* `file_type` - a string indicating the file that the status data refers to. Possible values are:
	* `D1` - D1 file
	* `D2` - D2 file
	* `E` - E file
	* `F` - F file

* `url` - a signed S3 URL from which the generated file can be downloaded
	* Blank string when the file is not `finished`

* `start` - **expected for D1/D2 only** - the file start date, in `MM/DD/YYYY` format
	* If the file is not a D1/D2 file type, return a blank string
* `end` - **expected for D1/D2 only** - the file end date, in `MM/DD/YYYY` format
	* If the file is not a D1/D2 file type, return a blank string

* `message` - returns a user-readable error message when the file is `failed`, otherwise returns a blank string


## Generate Detached Files (independent from a submission)
**Route:** `/v1/generate_detached_file`

**Method:** `POST`

This route sends a request to the backend to utilize the relevant external APIs and generate the relevant file for the metadata that is submitted.

### Body (JSON)

```
{
    "file_type": "D1",
    "cgac_code": "020",
    "start": "01/01/2016",
    "end": "03/31/2016"
}
```

### Body Description

* `file_type` - **required** - a string indicating the file type to generate. Allowable values are:
	* `D1` - generate a D1 file
	* `D2` - generate a D2 file
* `cgac_code` - **required for D1/D2 only** - the cgac of the agency for which to generate the files for
* `start` - **required for D1/D2 only** - the start date of the requested date range, in `MM/DD/YYYY` string format
* `end` - **required for D1/D2 only** - the end date of the requested date range, in `MM/DD/YYYY` string format

### Response (JSON)
Response will be the same format as those which are returned in the `/v1/check_detached_generation_status` endpoint


## File Status
**Route:** `/v1/check_detached_generation_status`

**Method:** `POST`

This route returns either a signed S3 URL to the generated file or, if the file is not yet ready or have failed to generate for other reasons, returns a status indicating that.

### Body (JSON)

```
{
    "job_id": "1
}
```

### Body Description

* `job_id` - **required** - an integer corresponding the job_id for the generation. Provided in the response of the call to `generate_detached_file`

### Response (JSON)

*State:* The file has successfully generated

```
{
	"status": "finished",
	"file_type": "D1",
	"url": "https://........",
	"start": "01/01/2016",
	"end": "03/31/2016",
	"message": "",
	"job_id": 1
}
```

*State:* The file is not yet ready

```
{
	"status": "waiting",
	"file_type": "D1",
	"url": "",
    "start": "01/01/2016",
    "end": "03/31/2016",
    "message": "",
	"job_id": 1
}
```

*State:* File generation has failed

```
{
	"status": "failed",
	"file_type": "D1",
	"url": "",
	"start": "01/01/2016",
	"end": "03/31/2016",
	"message": "The server could not reach the Federal Procurement Data System. Try again later.",
	"job_id": 1
}
```

*State:* No file generation request has been made for this submission ID before

```
{
	"status": "invalid",
	"file_type": "D1",
	"url": "",
	"start": "",
	"end": "",
	"message": "",
	"job_id": 1
}
```


### Response Description

The response is an object that represents that file's state.

* `status` - a string constant indicating the file's status.
	* Possible values are:
		* `finished` - file has been generated and is available for download
		* `failed` - an error occurred and the file generation or S3 upload failed, the generated file is invalid, or any other error
		* `invalid` - no generation request has ever been made for this submission ID before

* `file_type` - a string indicating the file that the status data refers to. Possible values are:
	* `D1` - D1 file
	* `D2` - D2 file

* `url` - a signed S3 URL from which the generated file can be downloaded
	* Blank string when the file is not `finished`

* `start` - **expected for D1/D2 only** - the file start date, in `MM/DD/YYYY` format
	* If the file is not a D1/D2 file type, return a blank string
* `end` - **expected for D1/D2 only** - the file end date, in `MM/DD/YYYY` format
	* If the file is not a D1/D2 file type, return a blank string

* `message` - returns a user-readable error message when the file is `failed`, otherwise returns a blank string

* `job_id` - job ID of the generation job in question


## Test Cases

### Integration Tests

To run the broker API integration tests, navigate to the project's test folder (`data-act-broker-backend/tests`) and type the following:

        $ python integration/runTests.py

To generate a test coverage report from the command line:

1. Make sure you're in the project's test folder (`data-act-broker-backend/tests`).
2. Run the tests using the `coverage` command: `coverage run integration/runTests.py`.
3. After the tests are done running, view the coverage report by typing `coverage report`. To exclude third-party libraries from the report, you can tell it to ignore the `site-packages` folder: `coverage report --omit=*/site-packages*`.

## D File Callback
**Route:** `/v1/complete_generation/\<generation_task_key\>/`

**Method:** `POST`

This route is used by the D File API to return a file location for generated D files.

### Body (JSON)

```
{
    "href": "http://..."
}
```

### Body Description

* `href' - Location where generated D file can be downloaded
