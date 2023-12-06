# Submission Process
**IMPORTANT NOTE**: When pinging for status in any step, do not do it constantly, limit it to once every 10 seconds or longer.

## Introduction
While the Submission API has been designed to be as easy to understand as possible, it is intended to be used by those who are familiar with REST APIs and their request and authentication types. The Submission API provides the same functionality offered at broker.usaspending.gov. If you are unable to use the inbound api at broker-api.usaspending.gov based on this guide, please submit feedback to [Service Desk](https://servicedesk.usaspending.gov). For general information about the Broker API and its usage, see [this](./doc/README.md#data-act-broker-route-documentation) document.

## Login Process

### Login to Max
- Step 1: Authenticate with MAX directly to obtain the `ticket` value for Step 2
    - Please refer to documentation provided by MAX.gov [here](./Using_Digital_Certificates_for_MAX_Authentication.pdf).
    - Information about requesting Data Broker permissions within MAX can be found [here](https://community.max.gov/x/fJwuRQ).
    - While we do not control MAX's login process, for simplicity purposes, here is a sample CURL request to the MAX login endpoint:
    ```
        curl -L -j -D - -b none 
            --cert max.crt 
            --key max.key 
           https://serviceauth.max.gov/cas-cert/login?service=https://broker-api.usaspending.gov
   ```
    
- **NOTE**: Do **NOT** end the above service parameter url with a "/"
- You would locate the `ticket` value in the `Location` header in the first header block returned by this request, i.e.,
    `Location=https://broker-api.usaspending.gov?ticket=ST-123456-abcdefghijklmnopqrst-login.max.gov`
- Step 2: call `/v1/max_login/` (POST) current broker login endpoint for logging into broker using MAX login. For details on its use, click [here](./doc/api_docs/login/max_login.md)
    - Be sure to use the provided ticket within 30 seconds to ensure it does not expire.

## DABS Submission Process

### Upload B and C Files
- Step 1: call `/v1/upload_dabs_files/` (POST) to create the submission.
	- For details on its use, click [here](./doc/api_docs/file/upload_dabs_files.md)
- File A will be generated automatically at this point.
- **NOTE**: If you would like to certify this submission, call `/v1/published_submissions/` (GET) to ensure there are no other submissions already published by the same agency in the same period.
    - For details on its use, click [here](./doc/api_docs/file/published_submissions.md)

### Validate A, B, C Files
- File-level validation begins automatically on upload completion.
- Check status of validations using `/v1/check_status/`. For details on its use, click [here](./doc/api_docs/file/check_status.md)
- Continue polling with `check_status` until the following keys have a `status` of `finished` or `failed`:
    - `appropriations`
    - `program_activity`
    - `award_financial`
    - **NOTE**: If any of these have a status of `ready` that means they were never started.
- To get a general overview of the number of errors/warnings in the submission, along with all other metadata, `/v1/submission_metadata/` can be called. For details on its use, click [here](./doc/api_docs/file/submission_metadata.md)
- To get detailed information on each of the jobs and the errors that occurred in each, `/v1/submission_data/` can be called. For details on its use, click [here](./doc/api_docs/file/submission_data.md)
- If there are any errors and more granular detail is needed, get the error reports by calling `/v1/report_url/`. For details on its use, click [here](./doc/api_docs/file/report_url.md). In this case, `cross_type` should not be used.
- If the automatically generated file A is not adequate for any reason, at this point a custom file A may be uploaded along with any files that have errors in them.
- If a reupload is needed for any of the files, begin again from `upload_dabs_files` with these changes:
    - Only pass the keys of the files being updated (e.g. if only appropriations needs a reupload, you will pass `appropriations: "FILENAME"` as an entry in the payload but not the other two.
    - Add the key `existing_submission_id` with the ID of the submission as the content (string).
    - Response will update to not include the IDs and keys for any files that were not resubmitted
- If for any reason one of the uploaded files need to be redownloaded, use the `/v1/get_file_url` route to get the signed url for it. For details on its use, click [here](./doc/api_docs/file/get_file_url.md)

### Generate D1, D2 Files
- D File generation must be manually started ONLY AFTER all errors in A, B, C files have been fixed (warnings are allowed)
- Step 1: Call `/v1/generate_file/` for each of the D files (two calls, one for D1 and one for D2). For details on its use, click [here](./doc/api_docs/generation/generate_file.md)
- Step 2: Poll `/v1/check_generation_status/` for each D file individually to see if generation has completed. Pinging should continue until status is not `waiting` or `running`. For details on its use, click [here](./doc/api_docs/generation/check_generation_status.md)

### Cross-file validations
- Cross-file validation begins automatically upon successful completion of D file generation (no errors)
- Poll using the `check_status` route in the same manner as described in `Validate A, B, C Files` except the key being looked at should be `cross`. The same endpoints can also be used to gather the submission metadata and cross-file job data.
- To get a specific error/warning file, call `/v1/report_url/`. For details on its use, click [here](./doc/api_docs/file/report_url.md). In this case, `cross_type` should be used.
- If a file needs to be fixed, follow the same steps as in the `Validate A, B, C Files` section

### Generate E, F Files
- Once cross-file validation completes with 0 errors (warnings are acceptable), E/F file generation can begin.
- Call `/v1/generate_file/` to generate E and F files, will require being called twice (once for E and once for F). For details on its use, click [here](./doc/api_docs/generation/generate_file.md)
- Poll `/v1/check_generation_status/` for each file individually to see if generation has completed. Pinging should continue until status is not `waiting` or `running`. For details on its use, click [here](./doc/api_docs/generation/check_generation_status.md)

### Review and Add Comments
- Once the E and F files are generated successfully, the results can be reviewed using a series of calls.
- To see what comments exist for each file, call `/v1/get_submission_comments`. For details on its use, click [here](./doc/api_docs/file/get_submission_comments.md)
- To get the total obligations throughout the file, call `/v1/get_obligations`. For details on its use, click [here](./doc/api_docs/file/get_obligations.md)
- To update the comments on files, call `/v1/update_submission_comments`. For details on its use, click [here](./doc/api_docs/file/update_submission_comments.md)

### Certify Submission
- Certification must be done through the broker website

## FABS Submission Process

### Upload FABS File
- Call `/v1/upload_fabs_file/`
- For details on its use, click [here](./doc/api_docs/file/upload_fabs_file.md)

### Validate FABS File
- Validations are automatically started once the upload completes.
- Check status of validations using `/v1/check_status/`. For details on its use, click [here](./doc/api_docs/file/check_status.md)
- Continue polling with `check_status` until the `fabs` key has a `status` of `finished` or `failed`.
    - **NOTE**: If it has a status of `ready` that means it was never started.
- To get a general overview of the number of errors/warnings in the submission, along with all other metadata, `/v1/submission_metadata/` can be called. For details on its use, click [here](./doc/api_docs/file/submission_metadata.md)
- To get detailed information on the validation job and the errors that occurred in it, `/v1/submission_data/` can be called. For details on its use, click [here](./doc/api_docs/file/submission_data.md)
- If there are any errors and more granular detail is needed, get the error reports by calling `/v1/report_url/`. For details on its use, click [here](./doc/api_docs/file/report_url.md). In this case, `cross_type` should not be used.
- If a reupload is needed, begin again from `upload_fabs_file` with these changes:
    - `upload_fabs_file` Payload:
        - `fabs`: string, name of file being uploaded
        - `existing_submission_id`: string, ID of the submission
- If for any reason the uploaded file needs to be redownloaded, use the `get_file_url` route to get the signed url for it. For details on its use, click [here](./doc/api_docs/file/get_file_url.md)

### Publish Submission
- Publishing must be done through the broker website
