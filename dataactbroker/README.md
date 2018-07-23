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
The `dataactbroker/handlers` folder contains the logic to handle requests that are dispatched from the `domainRoutes.py`, `fileRoutes.py`, `loginRoutes.py`, and `userRoutes.py` files. Routes defined in these files may include the `@requires_login` and `@requires_submission_perms` tags to the route definition. This tag adds a wrapper that checks if there exists a session for the current user and if the user is logged in, as well as checking the user's permissions to determine if the user has access to this route. If user is not logged in to the system or does not have access to the route, a 401 HTTP error will be returned. This tags are defined in `dataactbroker/permissions.py`.

`account_handler.py` contains the functions to check logins and to log users out.

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
This route sends a request to the backend with the ticket obtained from the MAX login endpoint in order to verify authentication and access to the Data Broker. If called by a service account, a certificate is required for authentication.

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
	"user_id": 42,
	"name": "John",
	"title": "Developer",
	"skip_guide": false,
	"website_admin": false,
	"affiliations": [
		{
			"agency_name": "Department of Labor (DOL)",
			"permission": "writer"
		}
	],
	"session_id": "ABC123",
	"message": "Login successful"
}
```

##### Response Description:
- `user_id`: int, database identifier of the logged in user, part of response only if login is successful
- `name`: string, user's name, part of response only if login is successful
- `title`: string, title of user, part of response only if login is successful
- `skip_guide`: boolean, indicates whether or not the user has requested to skip introductory materials, part of response only if login is successful
- `website_admin`: boolean, describes a super-user status, part of response only if login is successful
- `affiliations`: list, indicates which agencies this user is a part of and what permissions they have at that agency, part of response only if login is successful
    - `agency_name`: string, name of agency user is affiliated with
    - `permission`: string, permission type for user (reader, writer, submitter, website_admin, editfabs, fabs)
- `message`: string, login error response "You have failed to login successfully with MAX", otherwise says "Login successful"
- `errorType`: string, type of error, part of response only if login is unsuccessful
- `trace`: list, traceback of error, part of response only if login is unsuccessful
- `session_id`: string, a hash the application uses to verify that user sending the request is logged in, part of response only if login is successful

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
    ],
    "session_id": "ABC123"
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
If submitting files directly via the API, a call to this route should be of content type `"multipart/form-data"`, and should use @ notation for the values of the "appropriations", "program_activity" and "award_financial" keys, to indicate the local path to the files to be uploaded.

If using the frontend and passing filename strings rather than files, this route will return conflict free S3 URLs for uploading. Each key put in the request comes back with a url_key containing the S3 URL and a key_id containing the job id. A returning submission_id will also exist which acts as identifier for the submission.

In addition, with frontend use, a credentials object is also part of the returning request. This object provides temporary access to upload S3 Files using an AWS SDK. It contains the following: SecretAccessKey, SessionToken, Expiration, and AccessKeyId. It is important to note that the role used to create the credentials should be limited to just S3 access.

If using the API, this route will upload the files, then kick off the validation jobs. It will return the submission_id, which can be used for the `/v1/check_status/` route to poll for validation completion. 

If using the frontend, you will need to call /v1/finalize_job/ to kick off validation once upload is complete.


#### Additional Required Headers (Backend only):
- `Content-Type` - `"multipart/form-data"`

#### Request Parameters:
- `appropriations` - local path to file using @ notation
- `program_activity` - local path to file using @ notation
- `award_financial` - local path to file using @ notation
- `cgac_code` - **required if not FREC** string, CGAC of agency (null if FREC agency)
- `frec_code` - **required if not CGAC** string, FREC of agency (null if CGAC agency)
- `is_quarter` - boolean (true for quarterly submissions)
- `reporting_period_start_date` - string, starting date of submission (MM/YYYY)
- `reporting_period_end_date` - string, ending date of submission (MM/YYYY)

**NOTE**: for monthly submissions, start/end date are the same

#### Example Frontend Request Using Filenames:
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

#### Example Curl Request Using the API Method:
```curl -i -X POST 
        -H "x-session-id: abcdefg-1234567-hijklmno-89101112"  
        -H "Content-Type: multipart/form-data" 
        -F 'cgac_code=020' 
        -F 'frec_code=null' 
        -F 'is_quarter=true' 
        -F 'reporting_period_start_date=04/2018' 
        -F 'reporting_period_end_date=06/2018' 
        -F "appropriations=@/local/path/to/a.csv" 
        -F "award_financial=@/local/path/to/c.csv"  
        -F "program_activity=@/local/path/to/b.csv"
    /v1/submit_files/
```

#### Example Output Using the Backend API:
```json
{
  "success":"true",
  "submission_id": 123
}
```

#### Example Output Using the Frontend:
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

#### POST "/v1/upload_detached_file/"
If using the API, a call to this route should be of content type `"multipart/form-data"`, and should use @ notation for the value of the "fabs" key, to indicate the local path to the file to be uploaded.

This route will upload the file, then kick off the validation jobs. It will return the submission id, which can be used for the `/v1/check_status/` route to poll for validation completion.

#### Additional Required Headers (API Only):
       - `Content-Type`: `"multipart/form-data"`

#### Request Parameters:
       - `agency_code`: string, sub tier agency code. Required if existing_submission_id is not included
       - `cgac_code`: null, optional
       - `frec_code`: null, optional
       - `fabs`: local path to file using @ notation
       - `is_quarter`: boolean, false for FABS submissions
       - `reporting_period_start_date`: null, optional
       - `reporting_period_end_date`: null, optional
       - `existing_submission_id` : integer, id of previous submission, use only if submitting an update.

#### Example curl request:
```
        curl -i -X POST /
            -H "x-session-id: abcdefg-1234567-hijklmno-89101112"
            -H "Content-Type: multipart/form-data"
            -F 'agency_code=2000'
            -F "fabs=@/local/path/to/fabs.csv"
        /v1/upload_detached_file/
```

#### Example output:
```json
{
  "success":true,
  "submission_id":12
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
  "success": true,
  "submission_id": 123
}
```

#### GET "/v1/revalidation\_threshold/"
This endpoint returns the revalidation threshold for the broker application. This is the date that denotes the earliest validation date a submission must have in order to be certifiable.

##### Sample Request
`/v1/revalidation_threshold/`

##### Request Params
N/A

##### Response (JSON)
```
{
    "revalidation_threshold": "01/15/2017"
}
```

##### Response Attributes
- `revalidation_threshold`: string, the date of the revalidation threshold (MM/DD/YYYY)

##### Errors
Possible HTTP Status Codes:

- 403: Permission denied, user does not have permission to view this submission


#### GET "/v1/submission\_metadata/"
This endpoint returns metadata for the requested submission.

##### Sample Request
`/v1/submission_metadata/?submission_id=123`

##### Request Params
- `submission_id` - **required** - an integer representing the ID of the submission to get metadata for

##### Response (JSON)
```
{
    "cgac_code": "000",
    "frec_code": null,
    "agency_name": "Agency Name",
    "number_of_errors": 10,
    "number_of_warnings": 20,
    "number_of_rows": 3,
    "total_size": 1800,
    "created_on": "04/16/2018",
    "last_updated": "2018-04-16T18:48:09",
    "last_validated": "04/16/2018",
    "reporting_period": "Q2/2018",
    "publish_status": "unpublished",
    "quarterly_submission": false,
    "fabs_submission": true,
    "fabs_meta": {
        "valid_rows": 1,
        "total_rows": 2,
        "publish_date": null,
        "published_file": null
    }
}
```

##### Response Attributes
- `cgac_code`: string, CGAC of agency (null if FREC agency)
- `frec_code`: string, FREC of agency (null if CGAC agency)
- `agency_name`: string, name of the submitting agency
- `number_of_errors`: int, total errors in the submission
- `number_of_warnings`: int, total warnings in the submission
- `number_of_rows`: int, total number of rows in the submission including file headers
- `total_size`: int, total size of all files in the submission in bytes
- `created_on`: string, date submission was created (MM/DD/YYYY)
- `last_updated`: string, date/time any changes (including validations, etc) were made to the submission (YYYY-MM-DDTHH:mm:ss)
- `last_validated`: string, date the most recent validations were completed (MM/DD/YYYY)
- `reporting_period`: string, reporting period of the submission (Q#/YYYY for quarterly submissions, MM/YYYY for monthly)
- `publish_status`: string, whether the submission is published or not. Can contain only the following values:
    - `unpublished`
    - `published`
    - `updated`
    - `publishing`
- `quarterly_submission`: boolean, whether the submission is quarterly or monthly
- `fabs_submission`: boolean, whether the submission is FABS or DABS (True for FABS)
- `fabs_meta`: object, data specific to FABS submissions (null for DABS submissions)
    - `publish_date`: string, Date/time submission was published (H:mm(AM/PM) MM/DD/YYYY) (null if unpublished)
    - `published_file`: string, signed url of the published file (null if unpublished)
    - `total_rows`: int, total rows in the submission not including header rows
    - `valid_rows`: int, total number of valid, publishable row

##### Errors
Possible HTTP Status Codes:

- 400:
    - Missing `submission_id` parameter
    - Submission does not exist
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission


#### GET "/v1/submission\_data/"
This endpoint returns detailed validation job data for the requested submission.

##### Sample Request
`/v1/submission_data/?submission_id=123&type=appropriations`

##### Request Params
- `submission_id` - **required** - an integer representing the ID of the submission to get job data for
- `type` - **optional** - a string limiting the results in the array to only contain the given file type. The following are valid values for this:
    - `fabs` - only for FABS submissions
    - `appropriations` - A
    - `program_activity` - B
    - `award_financial` - C
    - `award_procurement` - D1
    - `award` - D2
    - `cross` - cross-file

##### Response (JSON)
```
{
    "jobs": [{
        'job_id': 520,
        'job_status': "finished",
        'job_type': "csv_record_validation",
        'filename': "original_file_name.csv",
        'file_size': 1800,
        'number_of_rows': 3,
        'file_type': "fabs",
        'file_status': "complete",
        'error_type': "row_errors",
        'error_data': [{
            'field_name': "recordtype",
            'error_name': "required_error",
            'error_description': "This field is required for all submissions but was not provided in this row.",
            'occurrences': "1",
            'rule_failed': "This field is required for all submissions but was not provided in this row.",
            'original_label': "FABSREQ3"
        }],
        'warning_data': [],
        'missing_headers': [],
        'duplicated_headers': []
    }]
}
```

##### Response Attributes
- `job_id `: int, database ID of the job
- `job_status`: string, status of the job. Can be any of the following values:
    - `waiting`
    - `ready`
    - `running`
    - `finished`
    - `invalid`
    - `failed`
- `job_type`: string, the type of validation the job is, can be either of the following values:
    - `csv_record_validation` - a single file validation
    - `validation` - the cross-file validations
- `filename`: string, the orignal name of the submitted file (null for cross-file)
- `file_size`: int, size of the file in bytes (null for cross-file)
- `number_of_rows`: total number of rows in the file including header row (null for cross-file)
- `file_type`: type of the file, can only be the following values
    - `fabs` - will be the only file for FABS submissions and will not be present in DABS submissions
    - `appropriations` - A
    - `program_activity` - B
    - `award_financial` - C
    - `award_procurement` - D1
    - `award` - D2
    - ` ` - Empty string is used for cross-file jobs
- `file_status`: string, indicates the status of the file. Can only be the following values
    - `complete`
    - `header_error`
    - `unknown_error`
    - `single_row_error`
    - `job_error`
    - `incomplete`
    - `encoding_error`
    - `row_count_error`
    - `file_type_error`
- `error_type`: string, the overall type of error in the validation job. Can only be the following values
    - `header_errors`
    - `row_errors`
    - `none`
- `error_data`: array, details of each error that ocurred in the submission. Each entry is an object with the following keys, all returned values are strings
    -  `field_name`: the fields that were affected by the rule separated by commas if there are multiple
    -  `error_name`: the name of the error type, can be any of the following values
        -  `required_error`
        -  `rule_failed`
        -  `type_error`
        -  `value_error`
        -  `read_error`
        -  `write_error`
        -  `length_error`
    -  `error_description`: a description of the `error_name`
    -  `occurrences`: the number of times this error ocurred in this file
    -  `rule_failed`: the full description of the rule that failed
    -  `original_label`: the rule label for the rule that failed
-  `warning_data`: array, details of each warning that ocurred in the submission. Each entry is an object containing the same keys as those found in `error_data` with the exception that `error_name` can only be `rule_failed`.
-  `missing_headers`: array, each entry is a string with the name of the header that was missing
-  `duplicated_headers`: array, each entry is a string with the name of the header that was duplicated

##### Errors
Possible HTTP Status Codes:

- 400:
    - Missing `submission_id` parameter
    - Submission does not exist
    - Invalid type parameter
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission


#### GET "/v1/check\_status/"
This endpoint returns the status of each file type, including whether each has errors or warnings and a message if one exists.

##### Sample Request
`/v1/check_status/?submission_id=123&type=appropriations`

##### Request Params
- `submission_id` - **required** - an integer representing the ID of the submission to get statuses for
- `type` - **optional** - a string limiting the results in the array to only contain the given file type. The following are valid values for this:
    - `fabs` - only for FABS submissions
    - `appropriations` - A
    - `program_activity` - B
    - `award_financial` - C
    - `award_procurement` - D1
    - `award` - D2
    - `cross` - cross-file
    - `executive_compensation` - E
    - `sub_award` - F

##### Response (JSON)

```
{
    "fabs": {
        "status": "finished",
        "message": "",
        "has_errors": false,
        "has_warnings": true
    }
}
```

##### Response Attributes
Response attributes change depending on the submission and type requested. If a specific type is requested, only one attribute matching the requested type will be included. If no type is specified and the submission is a DABS submission, all possible file types will be included. The possible attributes match the valid request types. See above for a full list.

The contents of each attribute are an object containing the following keys:

- `status`: string, indicates the current status of the file type. Possible values include:
    - `ready` - not yet started
    - `uploading` - the file is uploading
    - `running` - the jobs are running
    - `finished` - all associated jobs are complete
    - `failed` - one or more of the associated jobs have failed
- `message`: string, the message associated with a job if there is one
- `has_errors`: boolean, indicates if the file type has any errors in validation
- `has_warnings`: boolean, indicates if the file type has any warnings in validation

##### Errors
Possible HTTP Status Codes:

- 400:
    - Missing `submission_id` parameter
    - Submission does not exist
    - Invalid type parameter
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission


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

#### GET "/v1/get\_obligations/"
This endpoint gets total obligations and specific obligations.

##### Sample Request
`/v1/get_obligations/?submission_id=123`

##### Request Params
- `submission_id` - **required** - an integer representing the ID of the submission to get obligations for

##### Response (JSON)

```
{
  "total_obligations": 75000.01,
  "total_procurement_obligations": 32500.01,
  "total_assistance_obligations": 42500
}
```

##### Reponse Attributes
- `total_obligations` - value representing the total obligations for the requested submission
- `total_procurement_obligations` - value representing the total procurement obligations for the requested submission
- `total_assistance_obligations` - value representing the total assistance obligations for the requested submission

##### Errors
Possible HTTP Status Codes:

- 400:
    - Missing `submission_id` parameter
    - Submission does not exist
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission


#### GET "/v1/submission/\<int:submission\_id\>/narrative"
This endpoint retrieves existing submission narratives (explanations/notes for particular files).

##### Sample Request
`/v1/submission/123/narrative`

##### Request Params
- `submission_id` - **required** - an integer representing the ID of the submission to get obligations for. This is found within the url itself, not at the end as an explicit param.

##### Response (JSON)

```
{
  "A": "Text of A's narrative",
  "B": "These will be empty if no notes are present",
  "C": "",
  "D1": "",
  "D2": "",
  "E": "",
  "F": ""
}
```

##### Reponse Attributes
- `A` - narrative for file A (Appropriations)
- `B` - narrative for file B (Program Activity)
- `C` - narrative for file C (Award Financial)
- `D1` - narrative for file D1 (Award Procurement)
- `D2` - narrative for file D2 (Award Financial Assistance)
- `E` - narrative for file E (Executive Compensation)
- `F` - narrative for file F (Sub Award)

##### Errors
Possible HTTP Status Codes:

- 400: Submission does not exist
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission

#### POST "/v1/submission/\<int:submission\_id\>/narrative"
This endpoint sets the file narratives for a given submission.

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

##### Body Description
All content passed in the body is updated in the database. If an attribute is left out, it will be treated as if it's an empty string.

**Important:** All narratives must be included every time in order to be kept. An attribute with an empty string will result in that narrative being deleted. (e.g. A narrative for file A already exists. A narrative for file B is being added. Narratives for both files A and B must be sent).

- `A` - narrative for file A (Appropriations)
- `B` - narrative for file B (Program Activity)
- `C` - narrative for file C (Award Financial)
- `D1` - narrative for file D1 (Award Procurement)
- `D2` - narrative for file D2 (Award Financial Assistance)
- `E` - narrative for file E (Executive Compensation)
- `F` - narrative for file F (Sub Award)

##### Response (JSON)

```
{}
```

##### Response Attributes
N/A

##### Errors
Possible HTTP Status Codes:

- 400: Submission does not exist
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission

#### GET "/v1/submission/\<int:submission\_id\>/report\_url"
This endpoint requests the URL associated with a particular type of submission report. The provided URL will expire after roughly half an hour.

##### Sample Request
`/v1/submission/<int:submission_id>/report_url?warning=True&file_type=appropriations&cross_type=award_financial`

##### Request Params
- `submission_id` - **required** - an integer representing the ID of the submission to get a report url for
- `warning` - **optional** - the boolean value true if the report is a warning report; defaults to false
- `file_type` - **required** - designates the type of report you're seeking
    - `appropriations` - A
    - `program_activity` - B
    - `award_financial` - C
    - `award_procurement` - D1
    - `award` - D2
    - `executive_compensation` - E
    - `sub_award` - F
    - `fabs` - FABS
- `cross_type` - **optional** - if present, indicates that we're looking for a cross-validation report between `file_type` and this parameter. The following are the only valid pairings, all other combinations of `file_type` and `cross_type` will result in invalid urls:
    - `file_type`: "appropriations", `cross_type`: "program\_activity"
    - `file_type`: "program\_activity", `cross_type`: "award\_financial"
    - `file_type`: "award\_financial", `cross_type`: "award\_procurement"
    - `file_type`: "award\_financial", `cross_type`: "award" 

##### Response (JSON)

```
{
  "url": "https://........"
}
```

##### Response Attributes
- `url` - signed url for the submission report

##### Errors
Possible HTTP Status Codes:

- 400:
    - Missing `submission_id` or `file_type` parameter
    - Submission does not exist
    - Invalid `file_type`, `cross_type`, or `warning` parameter
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission


#### GET "/v1/get\_file\_url"
This endpoint returns the signed url for the uploaded/generated file of the requested type

##### Sample Request
`/v1/get_file_url?submission_id=123&file_type=A`

##### Request Params
- `submission_id` - **required** - an integer representing the ID of the submission to get metadata for
- `file_type` - **required** - a string representing the file letter for the submission. Valid strings are the following:
    - `A`
    - `B`
    - `C`
    - `D1`
    - `D2`
    - `E`
    - `F`
    - `FABS`

##### Response (JSON)
```
{
    "url": "https://......."
}
```

##### Response Attributes
- `url`: string, the signed url for the requested file

##### Errors
Possible HTTP Status Codes:

- 400:
    - No such submission
    - Invalid file type (overall or for the submission specifically)
    - Missing parameter
- 401: Login required
- 403: Do not have permission to access that submission

#### POST "/v1/submit_detached_file"

This route sends a request to the backend with ID of the FABS submission we're submitting in order to publish it.

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

#### GET "/v1/gtas_window"

This route checks if there is a gtas window currently open, and if it is returns the start and end date, else returns None

##### Body 

None

##### Response (JSON)

Returns a data object with start and end dates if it is a window, or a data object containing null if it is not a window

```
{
  data : {
    start_date: '2012-05-17',
    end_date: '2012-06-17'
  }
}
```
* `start_date` - The date that the window opens
* `end_date` - The date that the window closes

#### POST "/v1/restart_validation"

This route alters a submission's jobs' statuses and then restarts all validations for the specified submission.

##### Body (JSON)

```
{
  "submission_id": 1,
  "d2_submission": True
}
```

##### Body Description

* `submission_id` - **required** - an integer corresponding to the ID of the submission for which the validations should be restarted.
* `d2_submission` - a boolean indicating whether this is a dabs or fabs submission

##### Response (JSON)

```
{
  "message": "Success"
}
```
* `message` - A message indicating whether or not the action was successful. Any message other than "Success" indicates a failure.

## File Generation Routes

#### GET "/v1/list_submissions/"
List submissions for all agencies for which the current user is a member of. Optional query parameters are `?page=[page #]&limit=[limit #]&certified=[true|false]&d2_submission=[true|false]` which correspond to the current page number and how many submissions to return per page (limit). If the query parameters are not present, the default is `page=1`, `limit=5`, and if `certified` is not provided, all submissions will be returned containing a mix of the two. By default, the list will not include d2_submissions.

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
      "files": ["file1.csv", "file2.csv"],
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
      "files": ["file1.csv", "file2.csv"],
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

#### POST "/v1/list_certifications/"
List certifications for a single submission

### Body (JSON)

```
{
    "submission_id": 123
}
```

### Body Description

* `submission_id` - **required** - an integer corresponding the submission_id

### Response (JSON)

Successful response will contain the submission_id and a list of certifications.

```
{
    "submission_id": 7,
    "certifications": [{
        "certify_date": "2017-05-11 18:10:18",
        "certify_history_id": 4,
        "certifying_user": {
            "name": "User Name",
            "user_id": 1
        },
        "certified_files": [{
            "certified_files_history_id": 1,
            "filename": "1492041855_file_c.csv",
            "is_warning": False,
            "narrative": "Comment on the file"
            },
            {"certified_files_history_id": 1,
            "filename": "submission_7_award_financial_warning_report.csv",
            "is_warning": True,
            "narrative": None}
        ]},
        {"certify_date": "2017-05-08 12:07:18",
        "certify_history_id": 3,
        "certifying_user": {
            "name": "Admin User Name",
            "user_id": 2
        },
        "certified_files": [{
            "certified_files_history_id": 3,
            "filename": "1492041855_file_a.csv",
            "is_warning": False,
            "narrative": "This is also a comment"
            },
            {"certified_files_history_id": 6,
            "filename": "submission_280_cross_warning_appropriations_program_activity.csv",
            "is_warning": True,
            "narrative": None}
        ]}
    ]
}
```

Invalid submission_ids (nonexistant, not certified, or FABS submissions) will return a 400 error.

#### POST "/v1/get_certified_file/"
Get a signed url for a specified history item

### Body (JSON)

```
{
    "submission_id": 1,
    "certified_files_history_id": 7,
    "is_warning": True
}
```

### Body Description

* `submission_id` - **required** - an integer corresponding the submission_id
* `certified_files_history_id` - **required** - an integer corresponding the certified_files_history_id
* `is_warning` - a boolean to denote whether the file being grabbed is a warning file or uploaded file

### Response (JSON)

Successful response will contain the signed S3 URL for the file we're trying to access.

```
{
    "url": "https://........",
}
```

Invalid certified_files_history_id, requests for a file not related to the submission_id given, or requests for a file that isn't stored in the table will return a 400 error.

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
### POST "/v1/generate\_file"
This route sends a request to the backend to utilize the relevant external APIs and generate the relevant file for the metadata that is submitted. This route is used for file generation **within** a submission.

#### Sample Request Body (JSON)
```
{
    "submission_id": 123,
    "file_type": "D1"
    "start": "01/01/2016",
    "end": "03/31/2016"
}
```

#### Body Parameters

- `submission_id` - **required** - an integer representing the ID of the current submission
- `file_type` - **required** - a string indicating the file type to generate. Allowable values are:
    - `D1` - generate a D1 file
    - `D2` - generate a D2 file
    - `E` - generate a E file
    - `F` - generate a F file
- `start` - **required for D1/D2 only** - the start date of the requested date range, in `MM/DD/YYYY` string format, should not be passed for E/F generation
- `end` - **required for D1/D2 only** - the end date of the requested date range, in `MM/DD/YYYY` string format, should not be passed for E/F generation

#### Response (JSON)
Response will be the same format as those which are returned in the `/v1/check_generation_status` endpoint.

#### Errors
Possible HTTP Status Codes not covered by `check_generation_status` documentation:

- 400:
    - Start and end date not provided for D1/D2 generation
    - Start and end date not formatted properly


### POST "/v1/generate\_detached\_file"

This route sends a request to the backend to utilize the relevant external APIs and generate the relevant file for the metadata that is submitted. This route is used for file generation **independent** from a submission.

#### Body (JSON)

```
{
    "file_type": "D1",
    "cgac_code": "020",
    "start": "01/01/2016",
    "end": "03/31/2016"
}
```

#### Body Description

- `file_type` - **required** - a string indicating the file type to generate. Allowable values are:
    - `D1` - generate a D1 file
    - `D2` - generate a D2 file
- `cgac_code` - **required** - the cgac of the agency for which to generate the files for
- `start` - **required** - the start date of the requested date range, in `MM/DD/YYYY` string format
- `end` - **required** - the end date of the requested date range, in `MM/DD/YYYY` string format

#### Response (JSON)
Response will be the same format as those returned from `/v1/check_generation_status` endpoint with the exception that only D1 and D2 files will ever be present, never E or F.

#### Errors
Possible HTTP Status Codes not covered by `check_generation_status` documentation:

- 400:
    - Missing cgac or frec code
    - Missing start or end date


## File Status
### GET "/v1/check\_generation\_status"

This route returns either a signed S3 URL to the generated file or, if the file is not yet ready or have failed to generate for other reasons, returns a status indicating that. This route is used for file generation **within** a submission.

#### Sample Request
`/v1/check_generation_status/?submission_id=123&file_type=D1`

#### Request Params
- `submission_id` - An integer representing the ID of the current submission
- `file_type` - **required** - a string indicating the file type whose status we are checking. Allowable values are:
    - `D1` - generate a D1 file
    - `D2` - generate a D2 file
    - `E` - generate a E file
    - `F` - generate a F file

#### Response (JSON)

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

#### Response Attributes
The response is an object that represents that file's state.

- `job_id` - an integer, job ID of the generation job in question
- `status` - a string constant indicating the file's status. Possible values are:
    - `finished` - file has been generated and is available for download
    - `waiting` - file has either not started/finished generating or has finished generating but is not yet uploaded to S3
    - `failed` - an error occurred and the file generation or S3 upload failed, the generated file is invalid, or any other error
    - `invalid` - no generation request has ever been made for this submission ID before
- `file_type` - a string indicating the file that the status data refers to. Possible values are:
    - `D1` - D1 file
    - `D2` - D2 file
    - `E` - E file
    - `F` - F file
- `url` - a string containing a signed S3 URL from which the generated file can be downloaded. This will be the string `"#"` if the file is not in the `finished` state.
- `size` - always null, should be the size of the created file
- `start` - **expected for D1/D2 only** - the file start date, in `MM/DD/YYYY` format. If not a D1/D2 file, this will be a blank string.
- `end` - **expected for D1/D2 only** - the file end date, in `MM/DD/YYYY` format. If not a D1/D2 file, this will be a blank string.
- `message` - a string of a user-readable error message when the file is `failed`, otherwise returns a blank string

#### Errors
Possible HTTP Status Codes:

- 400:
    - Missing `submission_id` parameter
    - Submission does not exist
    - Invalid `file_type` parameter
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission


### GET "/v1/check\_detached\_generation\_status"

This route returns either a signed S3 URL to the generated file or, if the file is not yet ready or have failed to generate for other reasons, returns a status indicating that. This route is used for file generation **independent** from a submission.

#### Sample Request (JSON)
`/v1/check_detached_generation_status/job_id=1`

### Request Params
- `job_id` - **required** - an integer corresponding the job_id for the generation. Provided in the response of the call to `generate_detached_file`

#### Response (JSON)
Response will be the same format as those returned from `/v1/check_generation_status` endpoint with the exception that only D1 and D2 files will ever be present, never E or F.

#### Errors
Possible HTTP Status Codes:

- 400:
    - Missing `job_id` parameter
    - Submission does not exist
- 401: Login required


## Test Cases

### Integration Tests

To run the broker API integration tests, navigate to the project's test folder (`data-act-broker-backend/tests`) and type the following:

        $ python integration/runTests.py

To generate a test coverage report from the command line:

1. Make sure you're in the project's test folder (`data-act-broker-backend/tests`).
2. Run the tests using the `coverage` command: `coverage run integration/runTests.py`.
3. After the tests are done running, view the coverage report by typing `coverage report`. To exclude third-party libraries from the report, you can tell it to ignore the `site-packages` folder: `coverage report --omit=*/site-packages*`.
