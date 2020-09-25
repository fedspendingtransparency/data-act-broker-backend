# POST "/v1/upload\_fabs\_file/"
A call to this route should be of content type `"multipart/form-data"`, and, if using curl or a similar service, should use @ notation for the value of the "fabs" key, to indicate the local path to the file to be uploaded. Otherwise, should pass a file-like object.

This route will upload the file, then kick off the validation jobs. It will return the submission id.

## Additional Required Headers
- `Content-Type`: `"multipart/form-data"`

## Example Curl Request For New Submission
```
  curl -i -X POST /
      -H "x-session-id: abcdefg-1234567-hijklmno-89101112"
      -H "Content-Type: multipart/form-data"
      -F 'agency_code=2000'
      -F "fabs=@/local/path/to/fabs.csv"
    /v1/upload_fabs_file/
```

## Example Curl Request For Existing Submission
```
  curl -i -X POST /
      -H "x-session-id: abcdefg-1234567-hijklmno-89101112"
      -H "Content-Type: multipart/form-data"
      -F 'existing_submission_id=5'
      -F "fabs=@/local/path/to/fabs.csv"
    /v1/upload_fabs_file/
```

## Request Params
- `fabs`: (required, string) local path to file using @ notation
- `agency_code`: (string) sub tier agency code. Required if existing_submission_id is not included
- `existing_submission_id`: (integer) ID of previous submission, use only if submitting an update.

## Response (JSON)
```
{
  "success": true,
  "submission_id": 12
}
```

## Response Attributes
- `success`: (boolean) whether the creation/update was successful or not
- `submission_id`: (integer) submission ID of the created or updated submission

## Errors
Possible HTTP Status Codes:

- 400:
    - Missing parameter
    - Submission does not exist
- 401: Login required
- 403: Permission denied, user does not have permission to view this submission