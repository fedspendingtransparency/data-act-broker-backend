# The DATA Act Broker Repository

The DATA Act Broker repository is the API, which communicates to the web front end.

## Installation

For instructions on contributing to this project or running your own copy of the DATA Act broker, please refer to the [documentation in the DATA Act core responsitory](https://github.com/fedspendingtransparency/data-act-core/blob/master/doc/INSTALL.md "DATA Act broker installation guide").

## Project Layout

The repository has two major directories: scripts and handlers.

```
dataactbroker/
├── scripts/        (Install and setup scripts)
└── handlers/       (Route handlers)
```

### Scripts
The `/dataactbroker/scripts` folder contains the install scripts needed to setup the broker API for a local install. For complete instructions on running your own copy of the API and other DATA Act broker components, please refer to the [documentation in the DATA Act core responsitory](https://github.com/fedspendingtransparency/data-act-core/blob/master/doc/INSTALL.md "DATA Act broker installation guide").

### Handlers
The `dataactbroker\handlers` folder contains the logic to handle requests that are dispatched from the `loginRoutes.py`, `fileRoutes.py`, and 'userRoutes.py' files. Routes defined in these files may include the `@permissions_check` tag to the route definition. This tag adds a wrapper that checks if there exists a session for the current user and if the user is logged in, as well as checking the user's permissions to determine if the user has access to this route. If user is not logged in to the system or does not have access to the route, a 401 HTTP error will be returned. This tag is defined in `dataactbroker/permissions.py`. Cookies are used to keep track of sessions for the end user. Only a UUID is stored in the cookie.

`accountHandler.py` contains the functions to check logins and to log users out.

`fileHandler.py` contains functions for managing user file interaction. It creates all of the jobs that are part of the user submission and has query methods to get the status of a submission. In addition, this class creates downloadable links to error reports created by the DATA Act Validator.

In addition to these helper objects, the following sub classes also exist within the directory: `UserHandler`, `JobHandler`, `ErrorHandler`, and 'InterfaceHolder'. These classes extend the database connection objects that are located in the Core Repository. Extra query methods exist in these classes that are used exclusively by the Broker API.

## DATA Act Broker Route Documentation

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

#### POST "/v1/login/"
This route checks the username and password against a credentials file.  Accepts input as json or form-urlencoded, with keys "username" and "password".  

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
    "name": "John",
    "title":"Developer",
    "agency": "Department of Labor",
    "permissions" : [0,1]
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
    "agency": "Department of Labor",
    "permissions" : [0,1]
}
```

Permissions for the DATA Act Broker are list based. Each integer in the list corresponds with a permission.


| Permission Type  | Value |
| ------------- |-------------|
|User| 0|
|Admin  |1|


#### POST "/v1/register/"
Registers a user with a confirmed email.  A call to this route should have JSON or form-urlencoded with keys "email", "name", "agency", "title", and "password".  If email does not match an email that has been confirmed, a 400 will be returned.  This route can only be called after the `confirm_email_token` route. After a successful submission this route will require
`confirm_email_token` to be called again.


Example input:

```json
{
   "email":"user@agency.gov",
   "name":"user",
   "agency":"Data Act Agency",
   "title":"User Title",
   "password":"pass"
}
```

Example output:

```json
{
  "message":"Registration successful"
}
```

#### POST "/v1/change_status/"
Changes a user's status, used to approve or deny users.  This route requires an admin user to be logged in.  A call to this route should have JSON or form-urlencoded with keys "uid" and "new_status".  For typical usage, "new_status" should be either "approved" or "denied".

Example input:

```json
{
   "uid":"1234",
   "new_status":"approved"
}
```

Example output:

```json
{
  "message":"Status change successful"
}
```

#### POST "/v1/confirm_email/"
Create a new user and sends a confirmation email to their email address.  A call to this route should have JSON or form-urlencoded with key "email".

Example input:

```json
{
   "email":"user@agency.gov"
}
```

Example output:

```json
{
  "message":"Email Sent"
}
```

#### POST "/v1/confirm_email_token/"
Checks the token sent by email.  If successful, updates the user to email_confirmed.  A call to this route should have JSON or form-urlencoded with key "token". If the token is invalid a failure message is returned along with the error code. The email address will also be returned upon success.

Example input:

```json
{
   "token":"longRandomString"
}
```

Success Example output:

```json
{
  "errorCode":0,
  "message":"success",
  "email" : "emailAddress@email.com"
}
```

Failure Example output:

```json
{
  "errorCode":3,
  "message":"Link already used"
}
```

The following is a table with all of the messages and error code  

| ErrorCode  | Value |Message |
| ------------- |-------------|------------- |
|INVALID_LINK | 1| Invalid Link|
| LINK_EXPIRED   |2| Link Expired|
| LINK_ALREADY_USED  |3|Link already used|
| LINK_VALID   |0|success|






#### POST "/v1/confirm_password_token/"
Checks the token sent by email for password reset. A call to this route should have JSON or form-urlencoded with key "token". If the token is invalid a failure message is returned along with the error code. The email address will also be returned upon success.

Example input:

```json
{
   "token":"longRandomString"
}
```

Success Example output:

```json
{
  "errorCode":0,
  "message":"success",
  "email" : "emailAddress@email.com"
}
```

Failure Example output:

```json
{
  "errorCode":3,
  "message":"Link already used"
}
```

The following is a table with all of the messages and error code  

| ErrorCode  | Value |Message |
| ------------- |-------------|------------- |
|INVALID_LINK | 1| Invalid Link|
| LINK_EXPIRED   |2| Link Expired|
| LINK_ALREADY_USED  |3|Link already used|
| LINK_VALID   |0|success|




#### POST "/v1/list_users_with_status/"
List all users with specified status, typically used to review users that have applied for an account.  Requires an admin login.  A call to this route should have JSON or form-urlencoded with key "status".

Example input:

```json
{
   "status":"awaiting_approval"
}
```

Example output:

```json
{
  "users":[{"uid":1,"name":"user","email":"agency@user.gov","title":"User Title","agency":"Data Act Agency"},{"uid":2,"name":"user2","email":"","title":"","agency":""}]
}
```

#### GET "/v1/list_submissions/"
List all submissions by currently logged in user.

Example input:

None

Example output:

```json
{
  "submission_id_list":[1,2,3]
}
```

#### POST "/v1/set_password/"
Change specified user's password to new value.  User must have confirmed the token they received in same session to use this route.  A call to this route should have JSON or form-urlencoded with keys "uid" and "password".

Example input:

```json
{
   "token":"longRandomString"
}
```

Example output:

```json
{
  "message":"Password successfully changed"
}
```

#### POST "/v1/reset_password/"
Remove current password and send password with token for reset.  A call to this route should have JSON or form-urlencoded with key "email".

Example input:

```json
{
   "email":"user@agency.gov"
}
```

Example output:

```json
{
  "message":"Password reset"
}
```


### File Routes

#### GET "/"
This route confirms that the broker is running

Example input: None
Example output: "Broker is running"

#### GET "/<filename>"
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
This route is used to retrieve S3 URLs to upload files. Data should be either JSON or form-urlencoded with keys: ["appropriations", "award\_financial", "award", "program\_activity"], each with a filename as a value.

This route will also add jobs to the job tracker DB and return conflict free S3 URLs for uploading. Each key put in the request comes back with an url_key containing the S3 URL and a key\_id containing the job id. A returning submission\_id will also exist which acts as identifier for the submission.

A credentials object is also part of the returning request. This object provides temporarily access to upload S3 Files using an AWS SDK. It contains the following: SecretAccessKey, SessionToken, Expiration, and AccessKeyId.
It is important to note that the role used to create the credentials should be limited to just S3 access.

When upload is complete, the finalize\_submission route should be called with the job\_id.

Example input:

```json
{
  "appropriations":"appropriations.csv",
  "award_financial":"award_financial.csv",
  "award":"award.csv",
  "program_activity":"program_activity.csv"

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
A call to this route should have JSON or form-urlencoded with a key of "upload\_id" and value of the job id received from the submit_files route. This will change the status of the upload job to finished so that dependent jobs can be started.

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
#### POST "/v1/submission\_error_reports/"
A call to this route should have JSON or form-urlencoded with a key of "submission\_id" and value of the submission id received from the submit\_files route.  The response object will be JSON with keys of "job\_X\_error\_url" for each job X that is part of the submission, and the value will be the signed URL of the error report on S3. Note that for failed jobs (i.e. file-level errors), no error reports will be created.

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
  "job_3008_error_url": "https..."
}
```

#### POST "/v1/check_status/"
A call to this route will provide status information on all jobs associated with the specified submission.  The request should have JSON or form-urlencoded with a key "submission\_id".  The response will contain a key for each job ID, with values containing dictionaries which detail the status of that job (with keys "status", "job\_type", and "file\_type").  

Example input:

```json
{
  "submission_id":1610
}
```

Example output:  

```json
{  
  "3005": {
    "status": "running",
    "file_type": "appropriations",
    "job_type": "file_upload"
  },  
  "3006": {
    "status": "waiting",
    "file_type": "appropriations",
    "job_type": "csv_record_validation"
  },      
}
```



## Test Cases

To run the broker API unit tests, navigate to the main project folder (`data-act-broker`) and type the following:

        $ python tests/runTests.py

Before running test cases, [make sure the validator is running](https://github.com/fedspendingtransparency/data-act-core/blob/master/doc/INSTALL.md#run-broker-backend-applications "run the DATA Act broker backend apps").

To generate a test coverage report from the command line:

1. Make sure you're in the main project folder (`data-act-broker`).
2. Run the tests using the `coverage` command: `coverage run tests/runTests.py`.
3. After the tests are done running, view the coverage report by typing `coverage report`. To exclude third-party libraries from the report, you can tell it to ignore the `site-packages` folder: `coverage report --omit=*/site-packages*`.
