## Route documentation for Flask server for Data Act
In general, status codes returned are as follows:
* 200 if successful
* 400 if request is malformed
* 401 if username or password are incorrect, or session has expired
* 500 for server-side errors

#### GET "/"
This route confirms that the broker is running
#### POST "/v1/login/"
This route checks the username and password against a credentials file.  Accepts input as json or form-urlencoded, with keys "username" and "password".  
#### POST "/v1/logout/"
Logs the current user out, only the login route will be accessible until the next login.  If not logged in, just stays logged out.  Returns 200 in either case.
#### GET "/v1/session/"
Checks that the session is still valid.  Returns a 200, and a JSON with key "status" containing True if the session exists, and False if not.
#### POST "/v1/submit_files/"
This route is used to retrieve S3 URLs to upload files to.  Data should be either JSON or form-urlencoded with keys: ["appropriations","award_financial","award","procurement"] each with a filename as a value.  
Route will add jobs to the job tracker DB and generate signed S3 URLs for uploading.  Each key put in comes back with key_url containing the S3 URL and key_id containing the job id.  When upload is complete, the finalize_submission route should be called with the job id.
#### POST "/v1/finalize_job/"
A call to this route should have JSON or form_urlencoded with a key of "upload_id" and value of the job id received from the submit_files route.  This will change the status of the upload job to finished so that jobs dependent on it can be started.
#### POST "/v1/submission_error_reports/"
A call to this route should have JSON or form_urlencoded with a key of "submission_id" and value of the submission id received from the submit_files route.  The response object will be JSON with keys of "job_X_error_url" for each job X that is part of the submission, and value will be a signed URL to the error report on S3.  Note that for failed jobs (i.e. file-level errors), no error reports will be created.
#### POST "/v1/check_status/"
A call to this route will provide status information on all jobs associated with the specified submission.  The request should have JSON or form_urlencoded with a key "submission_id".  The response will contain a key for each job ID, with values being dictionaries detailing the status of that job (with keys "status", "job_type", and "file_type").  