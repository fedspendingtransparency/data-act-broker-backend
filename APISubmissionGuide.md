# Submission Process
- **NOTE**: When pinging for status in any step, do not do it constantly, limit it to once every 10 seconds or longer.

## Login Process

### Login to Max
- Step 1: Authenticate with MAX directly to obtain the `ticket` value for Step 2
    - Please refer to documentation provided by MAX.gov [here](./Using_Digital_Certificates_for_MAX_Authentication.pdf).
    - While we do not control MAX's login process, for simplicity purposes, here is a sample CURL request to the MAX login endpoint:
    ```
        curl -L -j -D - -b none 
            --cert max.crt 
            --key max.key 
           https://piv.max.gov/cas/login?service=https://broker.usaspending.gov

    ```
    - You would locate the `ticket` value in the `Location` header in the first header block returned by this request, i.e.,
    `Location=https://broker-dev.usaspending.gov?ticket=ST-123456-abcdefghijklmnopqrst-login.max.gov`
- Step 2: call `/v1/max_login/` (POST) current broker login endpoint for logging into broker using MAX login. For details on its use, click [here](./dataactbroker/README.md#post-v1max_login)

## DABS Submission Process

### Upload A, B, C Files
- Step 1: call `/v1/upload_dabs_files/` (POST) to create the submission.
- For details on its use, click [here](./dataactbroker/README.md#post-v1upload_dabs_files)

### Validate A, B, C Files
- File-level validation begins automatically on upload completion.
- Check status of validations using `/v1/check_status/`. For details on its use, click [here](./dataactbroker/README.md#get-v1check_status)
- Continue polling with `check_status` until the following keys have a `status` of `finished` or `failed`:
    - `appropriations`
    - `program_activity`
    - `award_financial`
    - **NOTE**: If any of these have a status of `ready` that means they were never started.
- To get a general overview of the number of errors/warnings in the submission, along with all other metadata, `/v1/submission_metadata/` can be called. For details on its use, click [here](./dataactbroker/README.md#get-v1submission_metadata)
- To get detailed information on each of the jobs and the errors that occurred in each, `/v1/submission_data/` can be called. For details on its use, click [here](./dataactbroker/README.md#get-v1submission_data)
- If there are any errors and more granular detail is needed, get the error reports by calling `/v1/submission/SUBMISSIONID/report_url/`. For details on its use, click [here](./dataactbroker/README.md#get-v1submissionintsubmission_idreport_url). In this case, `cross_type` should not be used.
- If a reupload is needed for any of the files, begin again from `upload_dabs_files` with these changes:
    - Only pass the keys of the files being updated (e.g. if only appropriations needs a reupload, you will pass `appropriations: "FILENAME"` as an entry in the payload but not the other two.
    - Add the key `existing_submission_id` with the ID of the submission as the content (string).
    - Response will update to not include the IDs and keys for any files that were not resubmitted
    - Only call `finalize_job` on the updated files
- If for any reason one of the uploaded files need to be redownloaded, use the `/v1/get_file_url` route to get the signed url for it. For details on its use, click [here](./dataactbroker/README.md#get-v1get_file_url)

### Generate D1, D2 Files
- D File generation must be manually started ONLY AFTER all errors in A, B, C files have been fixed (warnings are allowed)
- Step 1: Call `/v1/generate_file/` for each of the D files (two calls, one for D1 and one for D2). For details on its use, click [here](./dataactbroker/README.md#post-v1generate_file)
- Step 2: Poll `/v1/check_generation_status/` for each D file individually to see if generation has completed. Pinging should continue until status is not `waiting` or `running`. For details on its use, click [here](./dataactbroker/README.md#post-v1check_generation_status)

### Cross-file validations
- Cross-file validation begins automatically upon successful completion of D file generation (no errors)
- Poll using the `check_status` route in the same manner as described in `Validate A, B, C Files` except the key being looked at should be `cross`. The same endpoints can also be used to gather the submission metadata and cross-file job data.
- To get a specific error/warning file, call `/v1/submission/SUBMISSIONID/report_url/`. For details on its use, click [here](./dataactbroker/README.md#get-v1submissionintsubmission_idreport_url). In this case, `cross_type` should be used.
- If a file needs to be fixed, follow the same steps as in the `Validate A, B, C Files` section

### Generate E, F Files
- Once cross-file validation completes with 0 errors (warnings are acceptable), E/F file generation can begin.
- Call `/v1/generate_file/` to generate E and F files, will require being called twice (once for E and once for F). For details on its use, click [here](./dataactbroker/README.md#post-v1generate_file)
- Poll `/v1/check_generation_status/` for each file individually to see if generation has completed. Pinging should continue until status is not `waiting` or `running`. For details on its use, click [here](./dataactbroker/README.md#post-v1check_generation_status)

### Review and Add Comments
- Once the E and F files are generated successfully, the results can be reviewed using a series of calls.
- To see what comments exist for each file, call `/v1/submission/SUBMISSIONID/narrative`. This is the GET version of this endpoint. For details on its use, click [here](./dataactbroker/README.md#get-v1submissionintsubmission_idnarrative)
- To get the total obligations throughout the file, call `/v1/get_obligations`. For details on its use, click [here](./dataactbroker/README.md#get-v1get_obligations)
- To update the comments on files, call `/v1/submission/SUBMISSIONID/narrative`. This is the POST version of this endpoint. For details on its use, click [here](./dataactbroker/README.md#post-v1submissionintsubmission_idnarrative)

### Certify Submission
- Certification must be done through the broker website

## FABS Submission Process

### Upload FABS File
- Call `/v1/upload_fabs_file/`
- For details on its use, click [here](./dataactbroker/README.md#post-v1upload_fabs_file)

### Validate FABS File
- Validations are automatically started once the upload completes.
- Check status of validations using `/v1/check_status/`. For details on its use, click [here](./dataactbroker/README.md#get-v1check_status)
- Continue polling with `check_status` until the `fabs` key has a `status` of `finished` or `failed`.
    - **NOTE**: If it has a status of `ready` that means it was never started.
- To get a general overview of the number of errors/warnings in the submission, along with all other metadata, `/v1/submission_metadata/` can be called. For details on its use, click [here](./dataactbroker/README.md#get-v1submission_metadata)
- To get detailed information on the validation job and the errors that occurred in it, `/v1/submission_data/` can be called. For details on its use, click [here](./dataactbroker/README.md#get-v1submission_data)
- If there are any errors and more granular detail is needed, get the error reports by calling `/v1/submission/SUBMISSIONID/report_url/`. For details on its use, click [here](./dataactbroker/README.md#get-v1submissionintsubmission_idreport_url). In this case, `cross_type` should not be used.
- If a reupload is needed, begin again from `upload_fabs_file` with these changes:
    - `upload_fabs_file` Payload:
        - `fabs`: string, name of file being uploaded
        - `existing_submission_id`: string, ID of the submission
- If for any reason the uploaded file needs to be redownloaded, use the `get_file_url` route to get the signed url for it. For details on its use, click [here](./dataactbroker/README.md#get-v1get_file_url)

### Publish Submission
- Publishing must be done through the broker website
