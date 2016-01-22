## Route documentation for Flask server for Data Act
In general, status codes returned are as follows:
* 200 if successful
* 400 if request is malformed
* 401 if username or password are incorrect, or session has expired
* 500 for server-side errors

#### GET "/"
This route confirms that the broker is running

Example input: None  
Example output: "Broker is running"

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

#### POST "/v1/submit_files/"
This route is used to retrieve S3 URLs to upload files.  Data should be either JSON or form-urlencoded with keys: ["appropriations","award_financial","award","procurement"] each with a filename as a value.  
The Route will also add jobs to the job tracker DB and return conflict free S3 URLs for uploading. Each key put in the request comes back with an url_key containing the S3 URL and a key_id containing the job id. A returning submission_id will also exist which acts as identifier for the submission.

A credentials object is also part of the returning request. This object provides temporarily access to upload S3 Files using an AWS SDK. It contains the following :SecretAccessKey , SessionToken, Expiration, and AccessKeyId.
It is important to note that the role used to create the credentials should be limited to just S3 access.

When upload is complete, the finalize_submission route should be called with the job id.

Example input:
{"appropriations":"appropriations.csv","award_financial":"award_financial.csv","award":"award.csv","procurement":"procurement.csv}  
Example output:  
{
  "submission_id": 12345,

  "bucket_name": "S3-bucket",

  "award_id": 100,
  "award_key": "2/1453474323_awards.csv",

  "appropriations_id": 101,
  "appropriations_key": "2/1453474324_appropriations.csv"


  "award_financial_id": 102,
  "award_financial_key": "2/1453474327_award_financial.csv",

  "procurement_id": 103,
  "procurement_key": "2/1453474333_procurement.csv"

  "credentials": {
    "SecretAccessKey": "ABCDEFG",
    "SessionToken": "ABCDEFG",
    "Expiration": "2016-01-22T15:25:23Z",
    "AccessKeyId": "ABCDEFG"
  },

}

#### POST "/v1/finalize_job/"
A call to this route should have JSON or form_urlencoded with a key of "upload_id" and value of the job id received from the submit_files route.  This will change the status of the upload job to finished so that jobs dependent on it can be started.

Example input: {"upload_id":3011}  
Example output: {"success": true}

#### POST "/v1/submission_error_reports/"
A call to this route should have JSON or form_urlencoded with a key of "submission_id" and value of the submission id received from the submit_files route.  The response object will be JSON with keys of "job_X_error_url" for each job X that is part of the submission, and value will be a signed URL to the error report on S3.  Note that for failed jobs (i.e. file-level errors), no error reports will be created.

Example input: {"submission_id":1610}  
Example output:  
{
  "job_3012_error_url": "https...",
  "job_3006_error_url": "https...",
  "job_3010_error_url": "https...",
  "job_3008_error_url": "https..."
}

#### POST "/v1/check_status/"
A call to this route will provide status information on all jobs associated with the specified submission.  The request should have JSON or form_urlencoded with a key "submission_id".  The response will contain a key for each job ID, with values being dictionaries detailing the status of that job (with keys "status", "job_type", and "file_type").  

Example input: {"submission_id":1610}  
Example output:  
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
  ...}
