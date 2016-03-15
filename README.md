# The DATA Act Broker Repository

The DATA Act Broker repository is the API, which communicates to the web front end. The repository has two major directories: scripts and handlers.

```
dataactbroker/
├── scripts/        (Install and setup scripts)
└── handlers/       (Route handlers)
```

##Scripts
The `/dataactbroker/scripts` folder contains the install scripts needed to setup the Broker for a local install.  `configure.py` creates the various JSON files needed for running the Broker. The following JSON files are created during
the install process : `manager.json` and `web_api_configuration.json`. The `configure.py` script is called
by the `initialize` script. The script however, can be called by itself to setup the JSON.

```bash

sudo python configure.py

```

`manager.json` contains the web URL where the DATA Act validator exists. It has the following format.

```json
{
  "url":"http://server_url.com:5000"
}
```

`web_api_configurations.json` contains data used by the Data Broker Flask application for setting ports and debug options. It has the following format:

```json
{
  "rest_trace": false,
  "server_debug": false,
  "origins": "*",
  "port": 5000,
  "local_dynamo": false,
  "dynamo_port": 5000,
  "create_credentials": true
}
```

 The following table describes each setting in the configurations file:

| Setting  | Value |
| ------------- | ------------- |
| rest_trace  | Provides debug output to rest responses   |
| server_debug  | Turns on debug mode for the Flask server  |
| origins  | The URL that cross-origin HTTP requests are enabled on |
| local_dynamo  | Sets if the dynamo database is on the localhost or AWS|
| dynamo_port  | The port used for the dynamo database|
| create_credentials  | Turns on the ability to create temporarily AWS credentials|
| frontend_url  | The URL for the React front end|
| security_key  | The key used to make hashes by the application|
| system_email  | The from email address  used by the system for automated emails|

The `initialize` script provides users with these choices during the install process. See the [Broker Install Guide](#install-guide) for more information.

##Handlers
The `dataactbroker\handlers` folder contains the logic to handle requests that are dispatched from the `loginRoutes.py` and `fileRoutes.py` files. Routes defined in these files may include the `@permissions_check` tag to the route definition. This tag adds a wrapper that checks if there exists a session for the current user and if the user is logged in. If user is not logged in to the system, a 401 HTTP error will be returned. This tag is defined in `dataactbroker/permissions.py`. Cookies are used to keep track of sessions for the end user. Only a UUID is stored in the cookie.

`AccountHandler.py` contains the functions to check logins and to log users out.

`FileHandler.py` contains functions for managing user file interaction. It creates all of the jobs that are part of the user submission and has query methods to get the status of a submission. In addition, this class creates downloadable links to error reports created by the DATA Act Validator.

In addition to these helper objects, the following sub classes also exist within the directory: `UserHandler`, `JobHandler`, and `ErrorHandler`. These classes extend the database connection objects that are located in the Core Repository. Extra query methods exist in these classes that are used exclusively by the Broker API.


# AWS Setup
In order to use the DATA Act Broker, additional AWS permissions and configurations are
required in addition to those listed in the [DATA ACT Core README](https://github.com/fedspendingtransparency/data-act-core/blob/development/README.md).

## DynamoDB
The DATA Act Broker uses AWS DynamoDB for session handling. This provides a fast and reliable methodology to check sessions in the cloud. Users can easily bounce between servers with no impact to their session.

The install script seen in the [Broker Install Guide](#install-guide) provides an option to create the database automatically. This, however, assumes the user has the proper AWS Credentials to perform the operation. If you wish to create the database manually, it needs to be set up to have the following attributes:

| Setting  | Value | Type|
| ------------- | ------------- |-------------|
| Table Name | BrokerSession | N/A |
| Primary index  |  uid  | hashkey |
| Secondary Index | expiration-index | number |

### Role Permissions
The EC2 instance running the broker should be granted read/write permissions to DynamoDB. The following JSON can be added to the role to grant this access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "dynamodb:*"
      ],
      "Effect": "Allow",
      "Resource": "arn:aws:dynamodb:REGION:ACCOUNT_ID:table/BrokerSession"
    }
  ]
}
```
The `REGION` should be replaced with region of the AWS account and the `ACCOUNT_ID` should be replaced with the AWS account ID.

### Local Version

It is possible to set up DynamoDB locally. This requires Java JDK 6 or higher to be installed, which can be done using the following command on Red Hat based systems:

```bash
$ su -c "yum install java-1.7.0-openjdk"
```

For Ubuntu based systems the `apt-get` can be used instead

```bash
sudo apt-get install default-jre
```

Once Java is installed, you can download the local DynamoDB [here](http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.zip). Instructions to launch the local version one downloaded can be found in [AWS's User Guide](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html) along with the various options. Note that a local version of DynamoDB is **not** recommend for production.


## Assuming Roles
The DATA Act Broker uses the Assume Role method to create temporarily AWS credentials for the web front end. To be able to run the Broker locally, the user must be added to the Trust section of the S3 uploading role. Without adding this relationship, the Assume Role method call will fail. The following example shows what the JSON Trust Relationship should look like:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": [
          "arn:aws:iam::NUMBER:role/ec2rolename",
          "arn:aws:iam::NUMBER:user/user1",
          "arn:aws:iam::NUMBER:user/user2"
        ],
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}

```
`NUMBER` in the JSON above is your AWS account number. Each user and role must be stated. This example grants the role `ec2rolename`, as well as the users `user1` and `user2`.

# DATA Act Broker Route Documentation

## Status Codes
In general, status codes returned are as follows:

* 200 if successful
* 400 if the request is malformed
* 401 if the username or password are incorrect, or the session has expired
* 500 for server-side errors

## GET "/"
This route confirms that the broker is running

Example input:

None

Example output:

"Broker is running"

##User Routes

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


## File Routes


#### POST "/v1/local_upload/"
Input for this route should be a form with the key of `file` where the uploaded file is located. This route **only** will
return a success for local installs for security reasons. Upon successful upload, file path will be returned.

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



#### Test Cases
Before running test cases, start the Flask app by running `python app.py` in the `dataactbroker` folder. Alternatively, if using `pip install`, you can uses the server start command `sudo webbroker -s`. The current test suite for the validator may then be run by navigating to the `datatactbroker/tests folder` and running `python runTests.py`.


# Install Guide

## Requirements

DATA Act Broker is currently being built with Python 2.7. Before installing the Broker, please install the Data Act Core by following the [Data Act Core Installation Guide](https://github.com/fedspendingtransparency/data-act-core/blob/configuration/README.md).

## Install dependencies

### Python 2.7, `pip`, and AWS CLI Tools

Instructions for installing all three of these tools on your system can be found [here](https://docs.aws.amazon.com/cli/latest/userguide/installing.html#install-with-pip).

### Homebrew (Mac OS X)

We recommend using [homebrew](http://brew.sh) to install PostgreSQL for development on Mac OS X.

### PostgreSQL

[PostgreSQL](https://en.wikipedia.org/wiki/PostgreSQL) is an object-relational database management system (ORDBMS) with an emphasis on extensibility and standards-compliance, and is our database of choice for performing validations.

```bash
# Ubuntu/Linux 64-bit
$ sudo apt-get install postgresql postgresql-contrib

# Mac OS X
$ brew install postgresql
```

Upon install, follow the provided instructions to start postgres locally.

## Installing the Broker

Install the Broker and its dependencies with 'pip':

```bash
$ sudo pip install --process-dependency-links git+git://github.com/fedspendingtransparency/data-act-broker.git@configuration_feature
```

Note: we recommend [virtualenv](https://virtualenv.readthedocs.org/en/latest/installation.html) and [virtualenvwrapper](https://virtualenvwrapper.readthedocs.org/en/latest/install.html) to manage Python environments.

#### AWS CLI tools

Then, configure AWS using the CLI tools you installed earlier:

```bash
$ aws configure
// Enter your Access Key ID, Secret Access Key and region
```

#### Broker Configuration

Initialize the Broker and follow the prompted steps:

```bash
$ sudo webbroker –i
```

This command will let you setup the following:
- S3 configuration
- Database connection configuration
- Configure the Broker API
- Creates all database tables needed by the Broker

The following table below show the prompts created by the setup and there usage

| Prompt  | Value |
| ------------- | ------------- |
| Enter broker API port  | integer value, port 80 or 443 for http or https  |
| Would you like to enable server side debugging  | yes or no, turns on debug mode for the server. This should **not** be used in production.|
| Would you like to enable debug traces on REST requests  |yes or no, enables returning server error messages in the rest request. This should **not** be enabled in production|
| Would you like to use a local dynamo database  | yes or no, enables use of local dynamo database. This should be yes only if you do not have access to an AWS account.|
|Enter the port for the local dynamo database|integer, port 8000 is used by default. This prompt only appears when a local dynamo database is selected.|
| Enter the allowed origin (website that will allow for CORS)  | this is the website URL of the DATA Act Broker front end. * can be used in its place but this value should **not** be used in production|
|Would you like to create the dynamo database table|yes or no. Creates a table on the Dynamo Database. This command you be used **exactly once** per AWS account |
|Would you like to include test case users | yes or no, this options adds test users. This option should **not** be selected for production|
|Enter the admin user password| string, this is the user password needed to login into the API|
|Enter the admin user email| string, this is the user email address needed to login into the API|






Alternatively, if you do not need to configure everything, the following commands are also available.

| Flag  | Description |
| ------------- | ------------- |
| -aws  | Configures AWS settings  |
| -c    | Configures Broker settings, such as ports, debug flags, and local dynamo|
| -db   |Creates the database schema|
| -cdb   |Configures database connection|



Finally, once the Broker has been initialized, run the Broker with the following command:

```bash
$ sudo webbroker -s
```
