# The DATA Act Broker Repository

The DATA Act Broker repository is the API that is used to communicate   
to the web front end. It has two major directories scripts and handlers.

```
dataactbroker/
├── scripts/        (Install and setup scripts)
└── handlers/       (Route handlers)
```

##Scipts
 The `/dataactbroker/scripts` folder contains the install scripts needed
 to setup the broker for a local install.

 The `configure` script creates the various JSON files needed for
 running the broker. The following three JSON files are created : `manager.json`,
 `web_api_configureation.json` and `credentials.json`

 `manager.json` contains the web url where the DATA Act validator exists.
 It has the following format.

```json
{
  "URL":"http://server_url.com:5000"
}
```

`credentials.json` contains users and passwords that the broker use's to
authenticate sessions. This file will be removed in later versions of the broker
when users are authenticated using a database. The file has the following
format where any number of users can be added.

```json
{
   "user1":"password1",
  "user2":"password2"
}
```


`web_api_configureation.json` contains data used by the Data Broker Flask application
for setting ports and debug options. It has the following format:

```json
{
  "rest_trace":false,
  "server_debug" : false,
  "origins": "*",
  "port": 5000,
  "local_dynamo": false,
  "dynamo_port" :5000,
  "create_credentials": true
}
```

 The following table describes each setting.

| Setting  | Value |
| ------------- | ------------- |
| rest_trace  | Provides debug output to rest responses   |
| server_debug  | turns on debug mode for the Flask server  |
| local_dynamo  | sets if the dynamo database is on the local host or AWS|
| dynamo_port  | the port used for the dynamo database|
| create_credentials  | turns on the ability to create temporarily AWS credentials|
| origins  | The url that CORS is enabled on|

The `initialize` script provides users with choices of the install process
see the [Broker Install Guide](#install-guide) for more information.

##Handlers
The `dataactbroker\handlers` folder contains the logic to handle requests
that are dispatched from the `loginRoutes.py` or `fileRoutes.py` files.

`LoginHandler.py` contains the functions to check logins and to log user
out. Currently `credentials.json` defines which users exist within the system.

`FileHandler.py` contains functions for managing user file interaction.
It creates call of the the jobs that are part of the user submission and has
query methods to get the status of a submission. In addition, this
class provide downloadable links to error reports created by the
DATA Act validator.


In addition to these helper objects, the following sub classes also exist
within the directory: `UserHandler`, `LoginHandler`, `JobHandler`, and `ErrorHandler`.
These classes extend the database connection objects that are located in the Core Repository.
Extra query methods exist in these classes that are used exclusively by the Broker API.


#AWS Setup
In order to use the DATA Act Broker, additional AWS permissions and configurations are
required in addition to those listed in the DATA ACT Core README.

##Dyanmo Database
The DATA Act Broker uses AWS Dynamo Database for session handling. This provides a fast and reliable methodology to check sessions in the cloud. Users can easily bounce between servers with no impact to there session.

The install script seen in the [Broker Install Guide](#install-guide)  provides an option to create the database automatically. This
however assumes the machine has the proper AWS Credentials to preform the operation.

If you wish to create the database manually, it needs to be setup to have the following attributes .

| Setting  | Value | Type|
| ------------- | ------------- |-------------|
| Table Name  | BrokerSession   ||N/A|
| Primary index  |  uid  |hashkey|
| Secondary Index  |experation-index|number  |

### Role Permissions
The EC2 instance running the broker should be granted read/write permissions to the Dynamo Database.
The following JSON can be added to the role to grant this access.

```json

```

###Local Version

It is possible to setup the Dynamo Database locally. This requires Java to
be installed which can be done using the following command on Red Hat based systems.

```bash
$ su -c "yum install java-1.7.0-openjdk"
```

For Ubuntu based systems the `apt-get` can be used instead

```bash
   sudo apt-get install default-jre

```

Once Java is installed, you can download the Local Dynamo Database [here](http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.zip).instructions to launch the local version one downloaded can be found in (AWS's User Guide)[http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html].along with the various options.  Note that a local version of  Dynamo is *not* recommend for production.

## Assuming Roles
The DATA Act broker uses the assume role function to create temporarily AWS credentials for the web front end. To be able to run the broker locally the user must be added to the trust section of the s3 uploading role. Without adding this relationship, the assume role will fail the following Example shows what the JSON should look like.

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
`NUMBER` in the json above is your AWS account number. Each user and role must be stated. This example grants the role `ec2rolename`, `user1` and `user2` the ability to assume a this role.

# DATA Act Broker Route Documentation

## Status Codes
In general, status codes returned are as follows:
* 200 if successful
* 400 if request is malformed
* 401 if username or password are incorrect, or session has expired
* 500 for server-side errors

## GET "/"
This route confirms that the broker is running

Example input: None  
Example output: "Broker is running"


##Login Methods

#### POST "/v1/login/"
This route checks the username and password against a credentials file.  Accepts input as json or form-urlencoded, with keys "username" and "password".  

Example input: {"username": "user", "password": "pass"}  
Example output: {"message": "Login successful"}

#### POST "/v1/logout/"
Logs the current user out, only the login route will be accessible until the next login.  If not logged in, just stays logged out.  Returns 200 in either case.

Example input: None  
Example output: {"message": "Logout successful"}

#### GET "/v1/session/"
Checks that the session is still valid.  Returns a 200, and a JSON with key "status" containing True if the session exists, and False if not.

Example input: None  
Example output: {"status": "True"}


##File Methods

#### POST "/v1/submit_files/"
This route is used to retrieve S3 URLs to upload files.  Data should be either JSON or form-urlencoded with keys: ["appropriations","award_financial","award","procurement"] each with a filename as a value.  
The Route will also add jobs to the job tracker DB and return conflict free S3 URLs for uploading. Each key put in the request comes back with an url_key containing the S3 URL and a key_id containing the job id. A returning submission_id will also exist which acts as identifier for the submission.

A credentials object is also part of the returning request. This object provides temporarily access to upload S3 Files using an AWS SDK. It contains the following :SecretAccessKey , SessionToken, Expiration, and AccessKeyId.
It is important to note that the role used to create the credentials should be limited to just S3 access.

When upload is complete, the finalize_submission route should be called with the job id.

Example input:
```json
{
  "appropriations":"appropriations.csv",
  "award_financial":"award_financial.csv",
  "award":"award.csv",
  "procurement":"procurement.csv"

}

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

  "procurement_id": 103,
  "procurement_key": "2/1453474333_procurement.csv",

  "credentials": {
    "SecretAccessKey": "ABCDEFG",
    "SessionToken": "ABCDEFG",
    "Expiration": "2016-01-22T15:25:23Z",
    "AccessKeyId": "ABCDEFG"
  }
}
```

#### POST "/v1/finalize_job/"
A call to this route should have JSON or form_urlencoded with a key of "upload_id" and value of the job id received from the submit_files route.  This will change the status of the upload job to finished so that jobs dependent on it can be started.

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
A call to this route should have JSON or form_urlencoded with a key of "submission_id" and value of the submission id received from the submit_files route.  The response object will be JSON with keys of "job_X_error_url" for each job X that is part of the submission, and value will be a signed URL to the error report on S3.  Note that for failed jobs (i.e. file-level errors), no error reports will be created.

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
A call to this route will provide status information on all jobs associated with the specified submission.  The request should have JSON or form_urlencoded with a key "submission_id".  The response will contain a key for each job ID, with values being dictionaries detailing the status of that job (with keys "status", "job_type", and "file_type").  

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
