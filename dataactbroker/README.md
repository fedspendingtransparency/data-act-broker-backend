# The DATA Act Broker Application Programming Interface (API)

The DATA Act Broker API powers the DATA Act's data submission process.

## Background

The U.S. Department of the Treasury is building a suite of open-source tools to help federal agencies comply with the [DATA Act](http://fedspendingtransparency.github.io/about/ "Federal Spending Transparency Background") and to deliver the resulting standardized federal spending information back to agencies and to the public.

For more information about the DATA Act Broker codebase, please visit this repository's [main README](../README.md "DATA Act Broker Backend README").

**A Note on CGAC/FREC**: In the vast majority of cases, top-level agencies identify themselves for purposes of DABS submissions or detached D1/D2 file generation by their 3-digit CGAC code. CGAC are issued and managed by OMB and are updated yearly in the A-11 circular appendix C. The CGAC is equivalent to the treasury concept of the Agency Identifier (AID) embedded in all Treasury Account Symbols (TAS).

In a few cases, legitimately separate (at least for financial reporting purposes) agencies share a CGAC. To allow them to report as separate entities in the DATA Act Broker, we leveraged an internal Treasury element called the Financial Reporting Entity Code (FREC) that Treasury already uses to distinguish between these agencies with shared AID at the TAS level. This field comes from Treasury's CARS system.
These agencies, listed in the table below, should use this four-digit FREC code for purposes of identifying themselves in DABS instead of the CGAC they share with one or more agencies.
The following is the complete list of agencies supported under the FREC paradigm in DABS. These agencies should always identify themselves to the Broker with the 4-digit FREC code instead of the 3 digit CGAC they share with other agencies.

|SHARED CGAC|	AGENCY NAME|	AGENCY ABBREVIATION| Financial Reporting Entity Code (FREC)|
|-----------|-----------|---------------------|--------------------------------------|
|011|	EOP Office of Administration|	EOPOA|1100|
|011|	Peace Corps|	Peace Corps|	1125|
|011|Inter-American Foundation|	IAF|	1130|
|011|U.S. Trade and Development Agency|	USTDA|1133|
|011|	African Development Foundation |ADF|1136
|016|Department of Labor|DOL	|1601|
|016|	Pension Benefit Guaranty Corporation|PBGC|1602|
|033|	Smithsonian Institution	|SI|	3300|
|033|	John F. Kennedy Center For The Performing Arts|Kennedy Center|3301|
|033|	National Gallery of Art|	National Gallery|3302|
|033|	Woodrow Wilson International Center For Scholars|Wilson Center|3303|
|352|	Farm Credit Administration|	FCA|7801|
|352|	Farm Credit System Insurance Corporation|FCSIC|7802|
|537|	Federal Housing Finance Agency|	FHFA|9566|
|537|	Federal Housing Finance Agency Inspector General|FHFAIG|9573|


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
The `dataactbroker/handlers` folder contains the logic to handle requests that are dispatched from the `domain_routes.py`, `file_routes.py`, `login_routes.py`, and `user_routes.py` files. Routes defined in these files may include the `@requires_login` and `@requires_submission_perms` tags to the route definition. This tag adds a wrapper that checks if there exists a session for the current user and if the user is logged in, as well as checking the user's permissions to determine if the user has access to this route. If user is not logged in to the system or does not have access to the route, a 401 HTTP error will be returned. This tags are defined in `dataactbroker/permissions.py`.

`account_handler.py` contains the functions to check logins and to log users out.

`fileHandler.py` contains functions for managing user file interaction. It creates all of the jobs that are part of the user submission and has query methods to get the status of a submission. In addition, this class creates downloadable links to error reports created by the DATA Act Validator.

## DATA Act Broker Route Documentation

All routes that require a login should now be passed a header "x-session-id".  The value for this header should be taken
from the login route response header "x-session-id".

**All routes have an optional trailing slash, meaning the route will work with or without it.**

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
This route sends a request to the backend with the ticket obtained from the MAX login endpoint in order to verify authentication and access to the Data Broker. If called by a service account, a certificate is required for authentication. **IMPORTANT**: The ticket has a 30 second expiration window so it must be used immediately after being received in order for it to be valid.

#### Body (JSON)

```
{
    "ticket": "ST-123456-abcdefghijklmnopqrst-login.max.gov",
    "service": "https://broker-api.usaspending.gov"
}
```

#### Body Description

* `ticket` - ticket string received from MAX from initial login request (pending validation)
* `service` - URL encoded string that is the source of the initial login request. This may vary from the example based on the environment you are in.

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

### **THIS ENDPOINT IS FOR LOCAL DEVELOPMENT ONLY AND CANNOT BE USED TO AUTHENTICATE INTO BROKER IN PRODUCTION**

This route checks the username and password against a credentials file. It is used solely as a workaround for developing on a local instance of the broker to bypass MAX.gov login. Accepts input as json or form-urlencoded, with keys "username" and "password". See `current_user` docs for details.

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

#### POST "/v1/upload\_dabs\_files/"
A call to this route should be of content type `"multipart/form-data"`, and, if using curl or a similar service, should use @ notation for the values of the "appropriations", "program\_activity" and "award\_financial" keys, to indicate the local path to the files to be uploaded. Otherwise, should pass a file-like object.

This route will upload the files, then kick off the validation jobs. It will return the submission\_id.

For a new submission, all three files must be submitted. For corrections to an existing submission, one or more files must be submitted along with the `existing_submission_id` parameter.

For information on the CGAC and FREC parameters, see the note above in the "Background" section.

##### Additional Required Headers:
- `Content-Type`: `"multipart/form-data"`

##### Example Curl Request For New Submission:
```
curl -i -X POST 
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
    /v1/upload_dabs_files/
```

##### Example Curl Request For Existing Submission:
```
curl -i -X POST 
      -H "x-session-id: abcdefg-1234567-hijklmno-89101112"  
      -H "Content-Type: multipart/form-data" 
      -F 'existing_submission_id=5' 
      -F "appropriations=@/local/path/to/a.csv"
    /v1/upload_dabs_files/
```

##### Request Params:
- `cgac_code`: (required if not FREC, string) CGAC of agency (null if FREC agency)
- `frec_code`: (required if not CGAC, string) FREC of agency (null if CGAC agency)
- `appropriations`: (string) local path to file using @ notation
- `program_activity`: (string) local path to file using @ notation
- `award_financial`: (string) local path to file using @ notation
- `is_quarter`: (boolean) True for quarterly submissions
- `reporting_period_start_date`: (string) starting date of submission (MM/YYYY)
- `reporting_period_end_date`: (string) ending date of submission (MM/YYYY)
- `existing_submission_id`: (integer) ID of previous submission, use only if submitting an update.

**NOTE**: for monthly submissions, start/end date are the same

##### Response (JSON):
```
{
  "success":"true",
  "submission_id": 123
}
```

##### Response Attributes
- `success `: (boolean) whether the creation was successful or not
- `submission_id`: (integer) submission ID of the created or updated submission

##### Errors
Possible HTTP Status Codes:

- 400:
    - Missing parameter
    - Submission does not exist
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission


#### POST "/v1/upload\_fabs\_file/"
A call to this route should be of content type `"multipart/form-data"`, and, if using curl or a similar service, should use @ notation for the value of the "fabs" key, to indicate the local path to the file to be uploaded. Otherwise, should pass a file-like object.

This route will upload the file, then kick off the validation jobs. It will return the submission id.

##### Additional Required Headers:
- `Content-Type`: `"multipart/form-data"`

##### Example Curl Request For New Submission:
```
  curl -i -X POST /
      -H "x-session-id: abcdefg-1234567-hijklmno-89101112"
      -H "Content-Type: multipart/form-data"
      -F 'agency_code=2000'
      -F "fabs=@/local/path/to/fabs.csv"
    /v1/upload_fabs_file/
```

##### Example Curl Request For Existing Submission:
```
  curl -i -X POST /
      -H "x-session-id: abcdefg-1234567-hijklmno-89101112"
      -H "Content-Type: multipart/form-data"
      -F 'existing_submission_id=5'
      -F "fabs=@/local/path/to/fabs.csv"
    /v1/upload_fabs_file/
```

##### Request Params:
- `fabs`: (required, string) local path to file using @ notation
- `agency_code`: (string) sub tier agency code. Required if existing_submission_id is not included
- `existing_submission_id`: (integer) ID of previous submission, use only if submitting an update.

##### Response (JSON):
```
{
  "success":true,
  "submission_id":12
}
```

##### Response Attributes
- `success`: (boolean) whether the creation was successful or not
- `submission_id`: (integer) submission ID of the created or updated submission

##### Errors
Possible HTTP Status Codes:

- 400:
    - Missing parameter
    - Submission does not exist
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission


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

#### GET "/v1/latest\_certification\_period/"
This endpoint returns the latest certification period for the broker application.

##### Sample Request
`/v1/latest_certification_period/`

##### Request Params
N/A

##### Response (JSON)
```
{
    "quarter": 4,
    "year": 2019
}
```

##### Response Attributes
- `quarter`: (integer) the quarter of the latest certification period, or none if no period is found
- `year`: (integer) the fiscal year of the latest certification period, or none if no period is found

##### Errors
Possible HTTP Status Codes:

- 401: Login required

#### GET "/v1/window/"
This endpoint returns a list of temporary messages to display on the Broker frontend as a banner.

##### Sample Request
`/v1/window/`

##### Request Params
N/A

##### Response (JSON)
```
    "data": [
        {
            "banner_type": "warning",
            "start_date": "2019-12-01",
            "header": null,
            "end_date": "2019-12-09",
            "notice_block": true,
            "type": "dabs",
            "message": "Submissions cannot be certified until ..."
        },
        {
            "banner_type": "info",
            "start_date": "2019-12-01",
            "header": null,
            "end_date": "2019-12-09",
            "notice_block": false,
            "type": "all",
            "message": "As a result of an issue identified ..."
        }
    ]
}
```

##### Response Attributes
- `data`: (list) available banner messages to display on the site
    - `header`: (string) The header of the message.
    - `message`: (string) The specific message.
    - `banner_type`: (string) The type of banner. Values include:
        - `info`: for informational messages
        - `warning`: for more pressing messages
    - `type`: (string) Which pages to display the message. Values include:
        - `fabs`: for FABS pages
        - `dabs`: for DABS pages
        - `all`: for both
    - `start_date`: (string) When to start displaying the message.
    - `end_date`: (string) The last day to display the message.
    - `notice_block`: (boolean) Whether the frontend may block submissions during the message period.

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
- `file_size`: bigint, size of the file in bytes (null for cross-file)
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
**Deprecated, scheduled for removal, use `/v1/get_submission_comments`**

#### POST "/v1/submission/\<int:submission\_id\>/narrative"
**Deprecated, scheduled for removal, use `/v1/update_submission_comments`**

#### GET "/v1/get\_submission\_comments/"
This endpoint retrieves existing submission comments (explanations/notes for particular files).

##### Sample Request
`/v1/get_submission_comments/?submission_id=123`

##### Request Params
- `submission_id`: (required, string) an integer representing the ID of the submission to get obligations for.

##### Response (JSON)

```
{
  "A": "Text of A's comment",
  "B": "These will be empty if no notes are present",
  "C": "",
  "D1": "",
  "D2": "",
  "E": "",
  "F": ""
}
```

##### Reponse Attributes
- `A`: (string) comment for file A (Appropriations)
- `B`: (string) comment for file B (Program Activity)
- `C`: (string) comment for file C (Award Financial)
- `D1`: (string) comment for file D1 (Award Procurement)
- `D2`: (string) comment for file D2 (Award Financial Assistance)
- `E`: (string) comment for file E (Executive Compensation)
- `F`: (string) comment for file F (Sub Award)

##### Errors
Possible HTTP Status Codes:

- 400: Submission does not exist
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission

#### POST "/v1/update\_submission\_comments/"
This endpoint sets the file comments for a given submission.

##### Body (JSON)

```
{
  "submission_id": 1234,
  "A": "Some new text",
  "C": "We didn't include B",
  "D1": "",
  "D2": "",
  "F": "Or E, for some reason"
}
```

##### Body Description
All content passed in the body is updated in the database. If an attribute is left out, it will be treated as if it's an empty string.

**Important:** All comments must be included every time in order to be kept. An attribute with an empty string will result in that comment being deleted. (e.g. A comment for file A already exists. A comment for file B is being added. Comments for both files A and B must be sent).

- `submission_id`: (required, string) The ID of the submission whose comments are getting updated
- `A`: (string) comment for file A (Appropriations)
- `B`: (string) comment for file B (Program Activity)
- `C`: (string) comment for file C (Award Financial)
- `D1`: (string) comment for file D1 (Award Procurement)
- `D2`: (string) comment for file D2 (Award Financial Assistance)
- `E`: (string) comment for file E (Executive Compensation)
- `F`: (string) comment for file F (Sub Award)

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

#### GET "/v1/get\_comments\_file/"
This endpoint retrieves the url to the file containing all the file comments associated with this submission.

##### Sample Request
`/v1/get_comments_file/?submission_id=123`

##### Request Params
- `submission_id`: (required, integer) the ID of the submission to get the comments file for. 

##### Response (JSON)
```
{
    "url": "http://url.to.file/full/path.csv"
}
```

##### Response Attributes
- `url`: (string) the url to the comments file

##### Errors
Possible HTTP Status Codes:

- 400:
    - Missing `submission_id` parameter
    - Submission does not exist
    - Submission does not have any comments associated with it
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission

#### GET "/v1/submission/\<int:submission\_id\>/report\_url/"
This endpoint requests the URL associated with a particular type of submission report. The provided URL will expire after one minute.

##### Sample Request
`/v1/submission/<int:submission_id>/report_url/?warning=True&file_type=appropriations&cross_type=award_financial`

##### Request Params
- `submission_id` - **required** - an integer representing the ID of the submission to get a report url for
- `warning` - **optional** - the boolean value true if the report is a warning report; defaults to false
- `file_type` - **required** - designates the type of report you're seeking
    - `appropriations` - A
    - `program_activity` - B
    - `award_financial` - C
    - `award_procurement` - D1
    - `award` - D2
    - `fabs` - FABS
- `cross_type` - **optional** - if present, indicates that we're looking for a cross-validation report between `file_type` and this parameter. The following are the only valid pairings, all other combinations of `file_type` and `cross_type` will result in an error:
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
    - Invalid `file_type`, `cross_type` pairing
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission


#### GET "/v1/get\_file\_url/"
This endpoint returns the signed url for the uploaded/generated file of the requested type

##### Sample Request
`/v1/get_file_url/?submission_id=123&file_type=A`

##### Request Params
- `submission_id` - **required** - an integer representing the ID of the submission to get the file url for
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

#### GET "/v1/get\_detached\_file\_url/"
This endpoint returns the signed url for the generated file of the requested job

##### Sample Request
`/v1/get_detached_file_url/?job_id=123`

##### Request Params
- `job_id` - **required** - an integer representing the ID of the job to get the file url for

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
    - No such job ID
    - The job ID provided is not a detached file generation
    - Missing parameter
- 401: Login required

#### POST "/v1/publish\_fabs\_file/"

This route sends a request to the backend with ID of the FABS submission to publish.

##### Body (JSON)

```
{
    "submission_id": 7
}
```

##### Body Description

- `submission_id` - **required** - ID of the submission to publish

##### Response (JSON)

```
{
    "submission_id": 7
}
```

##### Response Attributes

- `submission_id` - the ID of the submission being published

##### Errors
Possible HTTP Status Codes:

- 400: Invalid submission, already published or currently publishing submission, different submission published the same rows between validation and this API call, different submission that shares valid rows with this one currently publishing, missing required parameter
- 401: Login required
- 500: Any other unexpected errors


#### POST "/v1/delete_submission/"

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


#### POST "/v1/certify\_submission/"
This route certifies the specified submission, if possible. If a submission has critical errors, it cannot be certified. Only quarterly submissions can be certified. Submission files are copied to a certified bucket on aws if it is a non-local environment.

##### Body (JSON)

```
{
  "submission_id": 1
}
```

##### Body Description
- `submission_id` - **required** - an integer corresponding to the ID of the submission that is to be certified.

##### Response (JSON)

```
{
  "message": "Success"
}
```

##### Response Attributes
- `message` - A message indicating whether or not the action was successful. Any message other than "Success" indicates a failure.

##### Errors
Possible HTTP Status Codes:

- 400: Submission does not exist, critical errors prevent the submission from being certified, submission is a monthly submission, submission is already certified, a validation was completed before the revalidation threshold or the start of the submission window for the submission's year/quarter, submission window for this year/quarter doesn't exist, a different submission for this period was already published
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission


#### GET "/v1/gtas_window/"

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

#### POST "/v1/restart\_validation/"

This route alters a submission's jobs' statuses so they are no longer complete (requiring a regeneration and revalidation for all steps), uncaches all generated files, then restarts A/B/C or FABS validations for the specified submission.

##### Body (JSON)

```
{
  "submission_id": 1,
  "d2_submission": True
}
```

##### Body Description

- `submission_id` - **required** - an integer corresponding to the ID of the submission for which the validations should be restarted.
- `d2_submission` - a boolean indicating whether this is a DABS or FABS submission (True for FABS), defaults to False when not provided

##### Response (JSON)

```
{
  "message": "Success"
}
```

##### Response Attributes
- `message` - A message indicating whether or not the action was successful. Any message other than "Success" indicates a failure.

##### Errors
Possible HTTP Status Codes:

- 400: Submission does not exist
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission

#### POST "/v1/list\_submissions/"
This endpoint lists submissions for all agencies for which the current user is a member of. Optional filters allow for more refined lists.

##### Body (JSON)

```
{
    "page": 2
    "limit": 5,
    "certified": "true",
    "sort": "modified",
    "order": "desc",
    "fabs": False,
    "filters": {
        "submission_ids": [123, 456],
        "last_modified_range": {
            "start_date": "01/01/2018",
            "end_date": "01/10/2018"
        },
        "agency_codes": ["123", "4567"],
        "file_names": ["file_a", "test"],
        "user_ids: [1, 2]
    }
}
```

##### Body Description

- `page` - **optional** - an integer representing the page of submissions to view (offsets the list by `limit * (page - 1)`). Defaults to `1` if not provided
- `limit` - **optional** - an integer representing the total number of results to see from this request. Defaults to `5` if not provided
- `certified` - **required** - a string denoting the certification/publish status of the submissions listed. Allowed values are:
    - `true` - only include submissions that have been certified/published
    - `false` - only include submissions that have never been certified/published
    - `mixed` - include both certified/published and non-certified/published submissions
- `sort` - **optional** - a string denoting what value to sort by. Defaults to `modified` if not provided. Valid values are:
    - `modified` - last modified date
    - `reporting` - reporting start date
    - `agency` - agency name
    - `submitted_by` - name of user that created the submission
    - `certified_date` - latest certified date
- `order` - **optional** - a string indicating the sort order. Defaults to `desc` if not provided. Valid values are:
    - `desc`
    - `asc`
- `fabs` - **optional** - a boolean indicating if the submissions listed should be FABS or DABS (True for FABS). Defaults to `False` if not provided.
- `filters` - **optional** - an object containing additional filters to narrow the results returned by the endpoint. Possible filters are:
    - `submission_ids` - an array of integers or strings that limits the submission IDs returned to only the values listed in the array.
    - `last_modified_range` - an object containing a start and end date for the last modified date range. Both must be provided if this filter is used.
        - `start_date` - a string indicating the start date for the last modified date range (inclusive) (MM/DD/YYYY)
        - `end_date` - a string indicating the end date for the last modified date range (inclusive) (MM/DD/YYYY)
    - `agency_codes` - an array of strings containing CGAC and FREC codes
    - `file_names` - an array of strings containing total or partial matches to file names (including timestamps), will match any file name including generated ones
    - `user_ids` - an array of integers or strings that limits the list of submissions to only ones created by users within the array.

##### Response (JSON)

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
      "status": "validation_successful",
      "last_modified": "2016-08-30 12:59:37.053424",
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
      "status": "file_errors",
      "last_modified": "2016-08-31 15:59:37.053424",
      "publish_status": "unpublished",
      "certifying_user": "",
      "certified_on": ""
    }
  ],
  "total": 2,
  "min_last_modified": "2016-08-30 12:59:37.053424"
}
```

##### Response Attributes

- `total` - An integer indicating the total submissions that match the provided parameters (including those that didn't fit within the limit)
- `min_last_modified` - A string indicating the minimum last modified date for submissions with the same type (FABS/DABS) and certify status (certified/published, unpublished, both) as the request (additional filters do not affect this number)
- `submissions` - An array of objects that contain details about submissions. Contents of each object are:
    - `submission_id` - an integer indicating ID of the submission
    - `reporting_start_date` - a string containing the start date of the submission (`YYYY-MM-DD`)
    - `reporting_end_date` - a string containing the end date of the submission (`YYYY-MM-DD`)
    - `user` - an object containing details of the user that created the submission:
        - `name` - a string containing the name of the user
        - `user_id` - an integer indicating the ID of the user
    - `files` - an array of file names associated with the submission
    - `agency` - a string containing the name of the agency the submission is for
    - `status` - a string containing the current status of the submission. Possible values are:
        - `failed`
        - `file_errors`
        - `running`
        - `waiting`
        - `ready`
        - `validation_successful`
        - `validation_successful_warnings`
        - `certified`
        - `validation_errors`
    - `last_modified` - a string containing the last time/date the submission was modified in any way (`YYYY-MM-DD HH:mm:ss`)
    - `publish_status` - a string indicating the publish status of the submission. Possible values are:
        - `unpublished`
        - `published`
        - `updated`
        - `publishing`
    - `certifying_user` - a string containing the name of the last user to certify the submission
    - `certified_on` - a string containing the last time/date the submission was certified. (`YYYY-MM-DD HH:mm:ss`)

##### Errors
Possible HTTP Status Codes:

- 400: Invalid types in a filter, invalid values in a filter, missing required parameter
- 401: Login required


#### GET "/v1/list\_submission\_users/"
This endpoint lists all users with submissions that the requesting user can view, sorted by user name.

##### Sample Request
`/v1/list_submission_users/?d2_submission=False`

##### Request Params
- `d2_submission` - **optional** - a boolean indicating if the submissions checked should be FABS or DABS (True for FABS). Defaults to `False` if not provided.

##### Response (JSON)

```json
{
  "users": [
    {
      "user_id": 4,
      "name": "Another User"
    },
    {
      "user_id": 1,
      "name": "User One"
    }
  ]
}
```

##### Response Attributes

- `users` - An array of objects that contain the user's ID and name:
    - `user_id` - an integer indicating ID of the user
    - `name` - a string containing the name of the user

##### Errors
Possible HTTP Status Codes:

- 401: Login required


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
            "comment": "Comment on the file"
            },
            {"certified_files_history_id": 1,
            "filename": "submission_7_award_financial_warning_report.csv",
            "is_warning": True,
            "comment": None}
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
            "comment": "This is also a comment"
            },
            {"certified_files_history_id": 6,
            "filename": "submission_280_cross_warning_appropriations_program_activity.csv",
            "is_warning": True,
            "comment": None}
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

#### GET "/v1/list\_agencies/"
Gets all CGACs/FRECs that the user has permissions for.

##### Sample Request
`/v1/list_agencies/`

##### Request Params
N/A

##### Response (JSON)
```
{
    "cgac_agency_list": [
        {
            "agency_name": "Sample Agency",
            "cgac_code": "000"
        },
        {
            "agency_name": "Sample Agency 2",
            "cgac_code": "999"
        }
    ],
    "frec_agency_list": [
        {
            "agency_name": "Sample FREC Agency",
            "frec_code": "0000"
        }
    ]
}
```

##### Response Attributes
- `cgac_agency_list`: (list[dict]) A list of all cgac agencies (cgac code and agency name) the user has permissions to access.
- `frec_agency_list `: (list[dict]) A list of all frec agencies (frec code and agency name) the user has permissions to access.

##### Errors
Possible HTTP Status Codes:

- 401: Login required

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

#### POST "/v1/email\_users/"
This endpoint sends an email of the specified template to the users provided.

##### Body (JSON)

```
{
  "submission_id": 1234,
  "email_template": "review_submission",
  "users": [1, 2]
}
```

##### Body Description
- `submission_id` - **required** - an integer representing the ID of the submission to email about
- `email_template` - **required** - a string representing the type of template to use in the email. Currently, only the following templates exist (case-sensitive):
    - `review_submission`
- `users` - **required** - a list of integers representing the IDs of the users to send the emails to

##### Response (JSON)
```
{
    "message": "Emails successfully sent"
}
```

##### Response Attributes
- `message`: A message indicating that the emails were sent

##### Errors
Possible HTTP Status Codes:

- 400:
    - Missing parameters
    - Submission does not exist
    - Submission somehow is not associated with valid CGAC/FREC
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission


## Generate Files
### POST "/v1/generate\_file/"
This route sends a request to the backend to utilize the relevant external APIs and generate the relevant file for the metadata that is submitted. This route is used for file generation **within** a submission.

#### Sample Request Body (JSON)
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

#### Body Parameters

- `submission_id` - **required** - an integer representing the ID of the current submission
- `file_type` - **required** - a string indicating the file type to generate. Allowable values are:
    - `D1` - generate a D1 file
    - `D2` - generate a D2 file
    - `E` - generate a E file
    - `F` - generate a F file
- `start` - **required for D1/D2 only** - the start date of the requested date range, in `MM/DD/YYYY` string format, should not be passed for E/F generation
- `end` - **required for D1/D2 only** - the end date of the requested date range, in `MM/DD/YYYY` string format, should not be passed for E/F generation
- `agency_type` - **optional, used only in D1/D2** - a string indicating if the file generated should be based on awarding or funding agency. Defaults to `awarding` if not provided. Only allowed values are:
    - `awarding`
    - `funding`
- `file_format` - **optional, used only in D1/D2** - a string indicating if the file generated should be a comma delimited csv or a pipe delimited txt. Defaults to `csv` if not provided. Only allowed values are:
    - `csv`
    - `txt`

#### Response (JSON)
Response will be the same format as those which are returned in the `/v1/check_generation_status/` endpoint.

#### Errors
Possible HTTP Status Codes not covered by `check_generation_status` documentation:

- 400:
    - Start and end date not provided for D1/D2 generation
    - Start and end date not formatted properly


### POST "/v1/generate\_detached\_file/"

This route sends a request to the backend to utilize the relevant external APIs and generate the relevant file for the metadata that is submitted. This route is used for file generation **independent** from a submission. For more details on how files are generated, see the [FileLogic.md](../FileLogic.md) file.

#### Body (JSON)

```
{
    "file_type": "D1",
    "cgac_code": "020",
    "start": "01/01/2016",
    "end": "03/31/2016",
    "year": 2017,
    "period": 3,
    "agency_type": "awarding",
    "file_format": "csv"
}
```

#### Body Description

- `file_type` - **required** - a string indicating the file type to generate. Allowable values are:
    - `D1` - generate a D1 file
    - `D2` - generate a D2 file
    - `A` - generate an A file
- `cgac_code` - **required if frec\_code not provided** - the cgac of the agency for which to generate the files for
- `frec_code` - **required if cgac\_code not provided** - the frec of the agency for which to generate the files for
- `start` - **required for D file generation** - the start date of the requested date range, in `MM/DD/YYYY` string format
- `end` - **required for D file generation** - the end date of the requested date range, in `MM/DD/YYYY` string format
- `year` - **required for A file generation** - an integer indicating the year for which to generate an A file
- `period` - **required for A file generation** - an integer indicating the period for which to generate an A file
    - Allowed values: 2-12
    - 2 indicates November of the previous year, 12 indicates September of the selected year
- `agency_type` - **optional, used only in D1/D2** - a string indicating if the file generated should be based on awarding or funding agency. Defaults to `awarding` if not provided. Only allowed values are:
    - `awarding`
    - `funding`
- `file_format` - **optional, used only in D1/D2** - a string indicating if the file generated should be a comma delimited csv or a pipe delimited txt. Defaults to `csv` if not provided. Only allowed values are:
    - `csv`
    - `txt`

#### Response (JSON)
Response will be the same format as those returned from `/v1/check_generation_status/` endpoint with the exception that only D1, D2, and A files will ever be present, never E or F.

#### Errors
Possible HTTP Status Codes not covered by `check_generation_status` documentation:

- 400:
    - Missing cgac or frec code
    - Missing start or end date


## File Status
### GET "/v1/check\_generation\_status/"

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


### GET "/v1/check\_detached\_generation\_status/"

This route returns either a signed S3 URL to the generated file or, if the file is not yet ready or have failed to generate for other reasons, returns a status indicating that. This route is used for file generation **independent** from a submission.

#### Sample Request (JSON)
`/v1/check_detached_generation_status/?job_id=1`

### Request Params
- `job_id` - **required** - an integer corresponding the job_id for the generation. Provided in the response of the call to `generate_detached_file`

#### Response (JSON)
Response will be the same format as those returned from `/v1/check_generation_status/` endpoint with the exception that only D1, D2, and A files will ever be present, never E or F.

#### Errors
Possible HTTP Status Codes:

- 400:
    - Missing `job_id` parameter
    - Submission does not exist
- 401: Login required

## Dashboard Routes

The following routes are primarily used by the frontend for analytical purposes.

### POST "/v1/historic\_dabs\_summary/"

This route returns a list of submission summary dicts corresponding to the filters provided.
Note: the results will only include the submissions the user has access to based on their MAX permissions.

#### Body (JSON)
```
{
    "filters": {
        "quarters": [1, 3],
        "fys": [2017, 2019],
        "agencies": ["089", "1125"]
    }
}
```

#### Body Description
- `filters`: (required, dict) used to filter the resulting summaries
    - `quarters`: (required, list[integer]) fiscal year quarters, ranging 1-4, or an empty list to include all.
    - `fys`: (required, list[integer]) fiscal years, ranging from 2017 through the current fiscal year,
              or an empty list to include all.
    - `agencies`: (required, list[string]) CGAC or FREC codes, or an empty list to include all.

#### Response (JSON)

```
[
    {
        "agency_name": "Peace Corps (EOP)",
        "submissions": [
            {
                "submission_id": 104,
                "certifier": "Administrator",
                "fy": 2019,
                "quarter": 3
            },
            ...
        ]
    },
    ...
]
```

#### Response Attributes
The response is a list of dicts representing the requested agencies and their submission summaries, each with the following attributes:

- `agency_name`:  (dict) the name of the requested agency
- `submissions`: (list) the submissions for that agency in the periods requested, each with the following attributes:
    - `submission_id`: (integer) the submission ID of the summary
    - `certifier`: (string) name of the submission certifier
    - `fy`: (integer) the fiscal year of the summary
    - `quarter`: (integer) the fiscal quarter of the summary

#### Errors
Possible HTTP Status Codes:

- 400:
    - Invalid `quarters` parameter
    - Invalid `fys` parameter
    - Invalid `agencies` parameter
    - Missing required parameter
- 401: Login required


### POST "/v1/get\_rule\_labels/"
Gets a list of error/warning lables that pertain to the filters provided.

#### Body (JSON)
```
    {
        "files": ["A", "B", "cross-AB"],
        "fabs": false,
        "error_level": "warning"
    }
```

#### Body Description
- `files`: (required, array) Lists the files to get rule labels for. If an empty list is provided, all rule labels that match the remaining filters will be returned. If retrieving rules for a FABS submission, send an empty files list. Capitalization matters. Allowed values are:
    - `A`: Appropriations
    - `B`: Program Activity
    - `C`: Award Financial
    - `cross-AB`: cross-file between Appropriations and Program Activity
    - `cross-BC`: cross-file between Program Activity and Award Financial
    - `cross-CD1`: cross-file between Award Financial and Award Procurement
    - `cross-CD2`: cross-file between Award Financial and Award Financial Assistance
- `fabs`: (boolean) Determines whether labels being gathered are for FABS or DABS rules. Defaults to false if not provided
- `error_level`: (string) Determines whether to provide error or warning rule labels. Defaults to `warning` if not provided. Allowed values:
    - `error`
    - `warning`
    - `mixed`

#### Response (JSON)
```
{
    "labels": ["A3", "A11"]
}
```

#### Response Attributes
- `labels`: (array) The list of rule labels (strings) that correspond to the values provided in the request

#### Errors
Possible HTTP Status Codes:

- 400:
    - Files provided for FABS rule list
    - Invalid file type provided
    - Invalid parameter type provided

### POST "/v1/historic\_dabs\_graphs/"

This route returns a list of submission graph dicts corresponding to the filters provided.
Note: the results will only include the submissions the user has access to based on their MAX permissions.

#### Body (JSON)
```
{
    "filters": {
        "quarters": [1, 3],
        "fys": [2017, 2019],
        "agencies": ["089", "1125"],
        "files": ["B"],
        "rules": ["B11.4", "B9"]
    }
}
```

#### Body Description
- `filters`: (required, dict) used to filter the resulting summaries
    - `quarters`: (required, list[integer]) fiscal year quarters, ranging 1-4, or an empty list to include all.
    - `fys`: (required, list[integer]) fiscal years, ranging from 2017 through the current fiscal year,
              or an empty list to include all.
    - `agencies`: (required, list[string]) CGAC or FREC codes, or an empty list to include all.
    - `files`: (required, list[string]) files, or an empty list to include all.
    - `rules`: (required, list[string]) validation rules, or an empty list to include all.

#### Response (JSON)

```
{
    "B": [
        {
            "submission_id": 1234,
            "agency": 097,
            "fy": 2017,
            "quarter": 1,
            "total_warnings": 519,
            "warnings": [
                {
                    "label": "B11.4",
                    "instances": 352,
                    "percent_total": 68
                }, {
                    "label": "B9",
                    "instances": 167,
                    "percent_total": 32
                }
            ]
        },
        ...
    ],
    "C": [
        {
            "submission_id": 1234,
            "agency": 012,
            "fy": 2017,
            "quarter": 1,
            "total_warnings": 389,
            "warnings": [
                 {
                     "label": "C12",
                     "instances": 389,
                     "percent_total": 100
                 }
            ]
        },
        ...
    ],
```

#### Response Attributes

The response is a dictionary of lists representing the submission graphs, each with a list of dicts with the 
following attributes:

- `submission_id`: (integer) the submission ID of the summary
- `agency`:  (dict) the submission's agency, with the following attributes
    - `name`: (string) the agency's name
    - `code`: (string) the agency's code
- `fy`: (integer) the fiscal year of the summary
- `quarter`: (integer) the fiscal quarter of the summary
- `total_warnings`: (integer) the total instances of warnings associated with this submission and file
- `warnings`: ([dict]) list of warning dicts with the following attributes:
    - `label`: (string) rule number/label
    - `instances`: (integer) instances of this specific warning for this file and submission
    - `percent_total`: (integer) the percent of instances for this warning compared to the rest of the file and submission


#### Errors
Possible HTTP Status Codes:

- 400:
    - Invalid parameter
    - Missing required parameter
- 401: Login required

### POST "/v1/historic\_dabs\_table/"
This route returns a list of warning metadata rows for the dashboard table. Filters allow for more refined lists.
Note: the results will only include the submissions the user has access to based on their MAX permissions.

#### Body (JSON)
```
{
    "filters": {
        "quarters": [1, 3],
        "fys": [2017, 2019],
        "agencies": ["089", "1125"],
        "files": ["B"],
        "rules": ["B11.4", "B9"]
    },
    "page": 1,
    "limit": 10,
    "sort": "rule_label",
    "order": "asc"
}
```

#### Body Description
- `filters`: (required, dict) used to filter the resulting error metadata list
    - `quarters`: (required, list[integer]) fiscal year quarters, ranging 1-4, or an empty list to include all.
    - `fys`: (required, list[integer]) fiscal years, ranging from 2017 through the current fiscal year,
              or an empty list to include all.
    - `agencies`: (required, list[string]) CGAC or FREC codes, or an empty list to include all.
    - `files`: (required, list[string]) files, or an empty list to include all.
    - `rules`: (required, list[string]) validation rules, or an empty list to include all.
- `page`: (integer) The page of warning metadata to view (offsets the list by `limit * (page - 1)`). Defaults to `1` if not provided
- `limit`: (integer) The total number of results to see from this request. Defaults to `5` if not provided
- `sort`: (string) What value to sort by. Defaults to `period` if not provided. NOTE: Each sort value has a secondary (and sometimes tertiary) sort value to break ties. Valid values are:
    - `period` - fiscal year/quarter (secondary: rule label)
    - `rule_label` - the label of the rule (e.g. `B9`) (secondary: period)
    - `instances` - the number of times this warning occurred in this submission/file (secondary: rule label, tertiary: period)
    - `description` - the description of the rule (secondary: period)
    - `file` - the names of the files in which the warning occurred (secondary: rule label)
- `order`: (string) The sort order. Defaults to `desc` if not provided. Valid values are:
    - `desc`
    - `asc`

#### Response (JSON)

```
{
    "results": [
        {
            "submission_id": 1234,
            "files": [
                {
                 "type": "B",
                 "filename": "adsifmaoidsfmoisdfm-B.csv"
                },
                {
                 "type": "C",
                 "filename": "adsifmaoidsfmoisdfm-C.csv"
                },
            ],
            "fy": 2017,
            "quarter": 1,
            "rule_label": "B9",
            "instance_count": 609,
            "rule_description": "lorem ipsum whatever"
        },
        {
            "submission_id": 1234,
            "files": [
                {
                 "type": "B",
                 "filename": "adsifmaoidsfmoisdfm-B.csv"
                }
            ],
            "fy": 2017,
            "quarter": 1,
            "rule_label": "B9",
            "instance_count": 609,
            "rule_description": "lorem ipsum whatever"
        },
        ...
    ],
    "page_metadata": {
        "total": 20,
        "page": 1,
        "limit": 10
    }
```

#### Response Attributes

The response is a dictionary of lists representing the submission graphs, each with a list of dicts with the 
following attributes:

- `results`: ([dict]) the list of row results
    - `submission_id`: (integer) the submission ID of the warning
    - `files`:  ([dict]) The name and file type of the warning
        - `type`: (string) the file's type
        - `filename`: (string) the file's original name
    - `fy`: (integer) the fiscal year of the warning
    - `quarter`: (integer) the fiscal quarter of the warning
    - `rule_label`: (string) the label associated with the warning
    - `instance_count`: (integer) the number of times the warning occurred within the submission/file
    - `rule_description`: (string) the text of the warning
- `page_metadata`: (dict) metadata associated with the table
    - `total`: (int) total number of warning metadata rows these filters found
    - `page`: (int) the current page requested by the user
    - `limit`: (int) the total number of results to display per page as requested by the user


#### Errors
Possible HTTP Status Codes:

- 400:
    - Invalid parameter
    - Missing required parameter
- 401: Login required

## Automated Tests

Many of the broker tests involve interaction with a test database. However, these test databases are all created and 
torn down dynamically by the test framework, as new and isolated databases, so a live PostgreSQL server is all that's
needed.

These types of tests _should_ all be filed under the `data-act-broker-backend/tests/integration` folder, however the 
reality is that many of the tests filed under `data-act-broker-backend/tests/unit` also interact with a database. 

So first, ensure your `dataactcore/local_config.yml` and `dataactcore/local_secrets.yml` files are configured to be 
able to connect and authenticate to your local Postgres database server as instructed in [INSTALL.md](../doc/INSTALL.md) 

**To run _all_ tests**
```bash
$ pytest
```

**To run just _integration_ tests**
```bash
$ pytest tests/integration/*
```

**To run just _Broker API_ unit tests**
```bash
$ pytest tests/unit/dataactbroker/*
```

To generate a test coverage report with the run, just append the `--cov` flag to the `pytest` command.
