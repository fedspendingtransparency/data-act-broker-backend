# Submission Process
- **NOTE**: When pinging for status in any step, do not do it constantly, limit it to once every 10 seconds or longer.

## Login Process

### Login to Max
- Step 1: call `/vi/max_cert_login` (POST) to login to MAX using a certificate **Not yet implemented**
    - Payload:
        - `cert_path`: string, absolute path of the MAX certificate file
        - `service`: string, broker website the user is attempting to access
    - Response:
        - `ticket`: string,represents the verification that a user successfully logged into max. The ticket comes from the url the Max login service sends
        - `service`: string, broker website the user is attempting to access

- Step 2: call `/v1/max_login/` (POST) current broker login endpoint for logging into broker using MAX login
    - Payload:
        - `ticket`: string, represents the verification that a user successfully logged into max. The ticket comes from the url the Max login service sends
        - `service`: string, broker website the user is attempting to access
    - Response:
        - `user_id`: int, database identifier of the logged in user, part of response only if login is successful
        - `name`: string, user's name, part of response only if login is successful
        - `title`: string, title of user , part of response only if login is successful
        - `skip_guide`: boolean, indicates whether or not the user has requested to skip introductory materials, part of response only if login is successful
        - `website_admin`: boolean, describes a super-user status, part of response only if login is successful
        - `affiliations`: list, indicates which agencies this user is a part of and what permissions they have at that agency, part of response only if login is successful
            - `agency_name`: string, name of agency user is affiliated with
            - `permission`: string, permission type for user (reader, writer, submitter, website_admin, fabs)
        - `message`: string, login error response "You have failed to login successfully with MAX", otherwise says "Login successful"
        - `errorType`: string, type of error, part of response only if login is unsuccessful
        - `trace`: list, traceback of error, part of response only if login is unsuccessful
        - `session-token` (**Not yet implemented**): string, a hash the application uses to verify that user sending the request is logged in, part of response only if login is successful

## DABS Submission Process

### Upload A, B, C Files
- Step 1: call `/v1/submit_files/` (POST) to create the submission
    - Header:
        - `X-Session-ID`: string, session token id
    - Payload:
        - `appropriations`: string, file A name
        - `program_activity`: string, file B name
        - `award_financial`: string, file C name
        - `cgac_code`: string, CGAC of agency (null if FREC agency)
        - `frec_code`: string, FREC of agency (null if CGAC agency)
        - `is_quarter`: boolean (true for quarterly submissions)
        - `reporting_period_start_date`: string, starting date of submission (MM/YYYY)
        - `reporting_period_end_date`: string, ending date of submission (MM/YYYY)
    - **NOTE**: for monthly submissions, start/end date are the same
    - Response:
        - `appropriations_id`: int, ID of file A upload
        - `appropriations_key`: string, path to file A within S3 bucket
        - `program_activity_id`: int, ID of file B upload
        - `program_activity_key`: string, path to file B within S3 bucket
        - `award_financial_id`: int, ID of file C upload
        - `award_financial_key`: string, path to file C within S3 bucket
        - `award_procurement_id`: int, ID of file D1 upload
        - `award_procurement_key`: string, path to file D1 within S3 bucket
        - `award_id`: int, ID of file D2 upload
        - `award_key`: string, path to file D2 within S3 bucket
        - `executive_compensation_id`: int, ID of file E upload
        - `executive_compensation_key`: string, path to file E within S3 bucket
        - `sub_award_id`: int, ID of file F upload
        - `sub_award_key`: string, path to file F within S3 bucket
        - `bucket_name`: string, name of bucket on S3 that files are stored in
        - `credentials`: object, the credentials to S3 (AccessKeyId, Expiration, SecretAccessKey, SessionToken)
        - `submission_id`: int, ID of the submission that was created
- Step 2: Upload A, B, C files to S3 (Process to be defined)
- Step 3: Call `/v1/finalize_job/` (POST) for each file (A, B, C)
    - Payload:
        - `upload_id`: int, ID of the file upload received from the `submit_files` response
    - Response:
        - `success`: boolean, successful or failed progression of the jobs

### Validate A, B, C Files
- File-level validation begins automatically on completion of `finalize_job` call
- Check status of validations using `/v1/check_status/`. For details on its use, click [here](./dataactbroker/README.md#get-v1check_status)
- Continue polling with `check_status` until the following keys have a `status` of `finished` or `failed`:
    - `appropriations`
    - `program_activity`
    - `award_financial`
    - **NOTE**: If any of these have a status of `ready` that means they were never started.
- To get a general overview of the number of errors/warnings in the submission, along with all other metadata, `/v1/submission_metadata/` can be called. For details on its use, click [here](./dataactbroker/README.md#get-v1submission_metadata)
- To get detailed information on each of the jobs and the errors that occurred in each, `/v1/submission_data/` can be called. For details on its use, click [here](./dataactbroker/README.md#get-v1submission_data)
- If there are any errors and more granular detail is needed, get the error reports by calling `/v1/submission/SUBMISSIONID/report_url/` (POST) where `SUBMISSIONID` is the ID of the submission
    - Payload:
        - `file_type`: string, type of the file to get warnings/errors for (see above types for valid options)
        - `warning`: boolean (`true` for warning files, `false` for error files)
    - Response:
        - `url`: string, direct url to the error/warning report
- If a reupload is needed for any of the files, begin again from `submit_files` with these changes:
    - Only pass the keys of the files being updated (e.g. if only appropriations needs a reupload, you will pass `appropriations: "FILENAME"` as an entry in the payload but not the other two.
    - Add the key `existing_submission_id` with the ID of the submission as the content (string).
    - Response will update to not include the IDs and keys for any files that were not resubmitted
    - Only call `finalize_job` on the updated files

### Generate D1, D2 Files
- D File generation must be manually started ONLY AFTER all errors in A, B, C files have been fixed (warnings are allowed)
- Step 1: Call `/v1/generate_file/` (POST) for each of the D files
    - Header:
        - `X-Session-ID`: string, session token id
    - Payload:
        - `start`: string, start date for D file (first day of the starting month provided in `submit_files`, MM/DD/YYYY)
        - `end`: string, end date for D file (last day of the ending month provided in `submit_files`, MM/DD/YYYY)
        - `file_type`: string, type of D file (can only be "D1" or "D2")
        - `submission_id`: string, ID of the submission that was created
    - Response:
        - `start`: string, start date for D file (MM/DD/YYYY)
        - `end`: string, end date for D file (MM/DD/YYYY)
        - `file_type`: string, type of D file being generated
        - `message`: string, response from the API if an error occurs, otherwise empty string
        - `size`: int, size of file (always null for some reason, should only be null until file is finished)
        - `status`: string, status of file generation
        - `url`: string, full url of the D file ("#" if not generated or error)
- Step 2: Poll `/v1/check_generation_status/` (POST) for each D file individually to see if generation has completed. Pinging should continue until status is not `waiting` or `running`
    - Payload:
        - `submission_id`: string, ID of the submission that was created
        - `file_type`: string, type of D file (can only be "D1" or "D2")
    - Response:
        - `start`: string, start date for D file (MM/DD/YYYY)
        - `end`: string, end date for D file (MM/DD/YYYY)
        - `file_type`: string, type of D file being generated
        - `message`: string, response from the API if an error occurs, otherwise empty string
        - `size`: int, size of file (always null for some reason, should only be null until file is finished)
        - `status`: string, status of file generation
        - `url`: string, full url of the D file ("#" if not generated or error)

### Cross-file validations
- Cross-file validation begins automatically upon successful completion of D file generation (no errors)
- Poll using the `check_status` route in the same manner as described in `Validate A, B, C Files` except the key being looked at should be `cross`. The same endpoints can also be used to gather the submission metadata and cross-file job data.
- To get a specific error/warning file, call `/v1/submission/SUBMISSIONID/report_url/`(POST) where SUBMISSIONID is the ID of the submission
    - Header:
        - `X-Session-ID`: string, session token id
    - Payload:
        - `file_type`: string, the base file being compared against
        - `cross_type`: string, the secondary file being compared against
        - `warning`: boolean (true for warning files, false for error files)
    - Response:
        - `url`: string, direct url to the error/warning report
    - Valid file/cross type pairings (all other pairings will result in invalid URLs):
        - `file_type`: "appropriations", `cross_type`: "program\_activity"
        - `file_type`: "program\_activity", `cross_type`: "award\_financial"
        - `file_type`: "award\_financial", `cross_type`: "award\_procurement"
        - `file_type`: "award\_financial", `cross_type`: "award"
- If a file needs to be fixed, follow the same steps as in the `Validate A, B, C Files` section

### Generate E, F Files
- Once cross-file validation completes with 0 errors (warnings are acceptable), E/F file generation can begin.
- Call `/v1/generate_file/` (POST) to generate E and F files, will require being called twice
    - Header:
        - `X-Session-ID`: string, session token id
    - Payload:
        - `start`: string, empty string
        - `end`: string, empty string
        - `file_type`: string, type of file (can only be "E" or "F" for this generation)
        - `submission_id`: string, ID of the submission that was created
    - Response:
        - `start`: string, empty string
        - `end`: string, empty string
        - `file_type`: string, type of file being generated
        - `message`: string, response from the API if an error occurs, otherwise empty string
        - `size`: int, size of file (always null for some reason, should only be null until file is finished)
        - `status`: string, status of file generation
        - `url`: string, full url of the D file ("#" if not generated or error)
- Poll `/v1/check_generation_status/` (POST) for each file individually to see if generation has completed. Pinging should continue until status is not `waiting` or `running`
    - Payload:
        - `submission_id`: string, ID of the submission that was created
        - `file_type`: string, type of file (can only be "E" or "F")
    - Response:
        - `file_type`: string, type of file being generated
        - `message`: string, response from the API if an error occurs, otherwise empty string
        - `size`: int, size of file (always null for some reason, should only be null until file is finished)
        - `status`: string, status of file generation
        - `url`: string, full url of the file ("#" if not generated or error)

### Review and Add Comments
- Once the E and F files are generated successfully, the results can be reviewed using a series of calls.
- To see what comments exist for each file, call `/v1/submission/SUBMISSIONID/narrative` (GET) where SUBMISSIONID is the ID of the submission
    - Response:
        - `A`: string, comment on file A (Appropriations)
        - `B`: string, comment on file B (Program Activity)
        - `C`: string, comment on file C (Award Financial)
        - `D1`: string, comment on file D1 (Award Procurement)
        - `D2`: string, comment on file D2 (Award Financial Assistance)
        - `D2_detached`: string, comment on detached D2 file (FABS)
        - `E`: string, comment on file E (Executive Compensation)
        - `F`: string, comment on file F (Sub Award)
- To get the total obligations throughout the file, call `/v1/get_obligations` (POST)
    - Header:
        - `X-Session-ID`: string, session token id
    - Payload:
        - `submission_id`: string, ID of the submission
    - Response:
        - `total_assistance_obligations`: float, total financial assistance obligations
        - `total_obligations`: float, total obligations (file C)
        - `total_procurement_obligations`: float, total procurement obligations
- To update the comments on files, call `/v1/submission/SUBMISSIONID/narrative` (POST) where SUBMISSIONID is the ID of the submission
    - Payload:
        - `A`: string, comment on file A (Appropriations)
        - `B`: string, comment on file B (Program Activity)
        - `C`: string, comment on file C (Award Financial)
        - `D1`: string, comment on file D1 (Award Procurement)
        - `D2`: string, comment on file D2 (Award Financial Assistance)
        - `D2_detached`: string, comment on detached D2 file (FABS)
        - `E`: string, comment on file E (Executive Compensation)
        - `F`: string, comment on file F (Sub Award)
    - Response:
        - Empty object (maybe an error? I can't make it error)
    - NOTE: All comments must be included every time, not only new ones, otherwise old comments will be overwritten with empty strings (e.g. A comment for file A already exists. A comment for file B is being added. Comments for both files A and B must be sent).

### Certify Submission
- Certification must be done through the broker website

## FABS Submission Process

### Upload FABS File
- Step 1: Call `/v1/upload_detached_file/` (POST) (Note, this is NOT the file upload)
    - Header:
        - `X-Session-ID`: string, session token id
    - Payload:
        - `agency_code`: string, sub tier agency code
        - `cgac_code`: null
        - `frec_code`: null
        - `fabs`: string, name of the file being uploaded (e.g. "sample_file.csv")
        - `is_quarter`: boolean, false for FABS submissions
        - `reporting_period_start_date`: null
        - `reporting_period_end_date`: null
    - Response:
        - `fabs_id`: int, ID of FABS upload
        - `fabs_key`: string, path to FABS within S3 bucket
        - `bucket_name`: string, name of bucket on S3 that files are stored in
        - `credentials`: object, the credentials to S3 (AccessKeyId, Expiration, SecretAccessKey, SessionToken)
        - `submission_id`: int, ID of the submission that was created
- Step 2: Upload FABS file (Process to be defined)
- Step 3: Call `/v1/finalize_job/` (POST) for the FABS file
    - Payload:
        - `upload_id`: int, ID of the file upload received from the `submit_files` response
    - Response:
        - `success`: boolean, successful or failed progression of the jobs

### Validate FABS File
- Validations are automatically started by `finalize_job`
- Check status of validations using `/v1/check_status/`. For details on its use, click [here](./dataactbroker/README.md#get-v1check_status)
- Continue polling with `check_status` until the `fabs` key has a `status` of `finished` or `failed`.
    - **NOTE**: If it has a status of `ready` that means it was never started.
- To get a general overview of the number of errors/warnings in the submission, along with all other metadata, `/v1/submission_metadata/` can be called. For details on its use, click [here](./dataactbroker/README.md#get-v1submission_metadata)
- To get detailed information on the validation job and the errors that occurred in it, `/v1/submission_data/` can be called. For details on its use, click [here](./dataactbroker/README.md#get-v1submission_data)
- If there are any errors and more granular detail is needed, get the error reports by calling `/v1/submission/SUBMISSIONID/report_url/` (POST) where `SUBMISSIONID` is the ID of the submission
    - Header:
        - `X-Session-ID`: string, session token id
    - Payload:
        - `file_type`: string, always "fabs" for FABS files
        - `warning`: boolean (`true` for warning files, `false` for error files)
    - Response:
        - `url`: string, direct url to the error/warning report
- If a reupload is needed, begin again from `upload_detached_file` with these changes:
    - `upload_detached_file` Payload:
        - `fabs`: string, name of file being uploaded
        - `existing_submission_id`: string, ID of the submission

### Publish Submission
- Publishing must be done through the broker website

## Pending Enahncements
- API File upload