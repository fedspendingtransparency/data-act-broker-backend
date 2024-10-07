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
  
### Login to the CAIA

- Step 1: Get access to Treasury Mulesoft Exchange to view the Data Broker Experience API.
    - **NOTE**: Ensure Step 1 and 2 is completed by a user that will manage/own your system account. If that role is passed onto another, the system account can still be used but any changes to that account may result in needing to repeat this process.
    - Visit the [Treasury Mulesoft Exchange](https://gov.anypoint.mulesoft.com/accounts/login/fs) and log in.
        - If you reach a page that says "an entitlement request as been submitted to enable privileged access", wait for a day or two for your access to be approved.
    - Once you're able to log in to the Treasury Mulesoft Exchange, visit the [Data Broker Experience API](https://gov.anypoint.mulesoft.com/exchange/bdc8b2f6-1876-4267-8ab1-cc4ccab4d7b8/data-act-broker-experience-api/minor/1.0/).
        - If you reach a forbidden page, reach out to the Service Desk that you need viewing access to the Data Broker Experience API. They will let you know when access has been granted.
    - When you're able to visit the Data Broker Experience API, move onto Step 2.
- Step 2: Request access to the Data Broker Experience API and obtain the `client_id`/`client_secret` credentials.
    - Go to the [Data Broker Experience API](https://gov.anypoint.mulesoft.com/exchange/bdc8b2f6-1876-4267-8ab1-cc4ccab4d7b8/data-act-broker-experience-api/minor/1.0/).
    - You will be prompted to log into CAIA with your PIV.
    - Once on the main page, you will see **"Request Access"** on the top right side.
        - For **"API Instance"**, select the version under **"Production"**.
        - For **"Application"**, select **"Create a new application"**.
            - For **"Application Name"**, use a name representing your agency application that will be accessing the Broker.
            - Do not fill in any other fields and click **"Create"**.
        - For **"SLA Tier"**, select **"API User"**.
        - Select **"Request Access"**.
        - This generates a `Client ID` and `Client Secret` which you can access via **"Exchange"** -> **"My Applications"**.
- Step 3: Request via Service Desk to register a system account to use the Broker API proxy. You will need to provide: 
    - Your `Client ID` generated from Step 1. 
    - Your organization/agency name that owns the system.
    - A system-wide email representing your system.
    - Once you have confirmation that your system account has been registered and approved, move onto Step 3.
- Step 4: Using the `client_id`/`client_secret` with the Broker.
    - Each call to the Broker below in this guide (or any other endpoint referenced in the documentation) will be called slightly different:
        - The root url will be replaced:
            - Before: `https://broker-api.usaspending.gov/`
            - After: `https://api.fiscal.treasury.gov/ap/prod/exp/v1/data-act-broker/`
        - Two new headers must be added in every request:
            - `client_id`: The `Client ID` copied from earlier.
            - `client_secret`: The `Client Secret` copied from earlier.
    - Simply call `/v1/active_user` (GET) to get a new session started and confirm the user is correct.
- Troubleshooting
    - When finally logging into the Broker using the new credentials and you receive a message:
        - **"Authentication denied"**: The `client_id`/`client_secret` headers were not included. Make sure to include them.
        - **"Invalid Client"**: Your `client_id`/`client_secret` credentials were provided but incorrect. Ensure the values are correct.

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
