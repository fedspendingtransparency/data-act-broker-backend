# POST "/v1/upload\_dabs\_files/"
A call to this route should be of content type `"multipart/form-data"`, and, if using curl or a similar service, should use @ notation for the values of the "appropriations", "program\_activity" and "award\_financial" keys, to indicate the local path to the files to be uploaded. Otherwise, should pass a file-like object.

This route will upload the files, then kick off the validation jobs. It will return the submission\_id.

For a new submission, all three files must be submitted. For corrections to an existing submission, one or more files must be submitted along with the `existing_submission_id` parameter.

For information on the CGAC and FREC parameters, see the note in the main README in the "Background" section.

## Additional Required Headers
- `Content-Type`: `"multipart/form-data"`

## Example Curl Request For New Submission
```
curl -i -X POST 
      -H "x-session-id: abcdefg-1234567-hijklmno-89101112"  
      -H "Content-Type: multipart/form-data" 
      -F 'cgac_code=020' 
      -F 'frec_code=null' 
      -F 'is_quarter=true' 
      -F 'test_submission=true'
      -F 'reporting_period_start_date=04/2018' 
      -F 'reporting_period_end_date=06/2018' 
      -F "appropriations=@/local/path/to/a.csv" 
      -F "award_financial=@/local/path/to/c.csv"  
      -F "program_activity=@/local/path/to/b.csv"
    /v1/upload_dabs_files/
```

## Example Curl Request For Existing Submission
```
curl -i -X POST 
      -H "x-session-id: abcdefg-1234567-hijklmno-89101112"  
      -H "Content-Type: multipart/form-data" 
      -F 'existing_submission_id=5' 
      -F "appropriations=@/local/path/to/a.csv"
    /v1/upload_dabs_files/
```

## Body Description
For monthly submissions, start/end date are the same except in the case of period 1/2, which must be done together (start: 10/YYYY, end: 11/YYYY)

- `cgac_code`: (required if not FREC, string) CGAC of agency (null if FREC agency)
- `frec_code`: (required if not CGAC, string) FREC of agency (null if CGAC agency)
- `appropriations`: (string) local path to file using @ notation
- `program_activity`: (string) local path to file using @ notation
- `award_financial`: (string) local path to file using @ notation
- `is_quarter`: (boolean) True for quarterly submissions. Defaults to false if not provided.
- `test_submission`: (boolean) True when you want to create a test submission. Defaults to false (will not update existing submissions)
- `reporting_period_start_date`: (string) starting date of submission (MM/YYYY)
- `reporting_period_end_date`: (string) ending date of submission (MM/YYYY)
- `existing_submission_id`: (integer) ID of previous submission, use only if submitting an update.

## Response (JSON)
```
{
  "success": "true",
  "submission_id": 123
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
    - Invalid start/end date combination
- 401: Login required
- 403: Permission denied, user does not have permission to edit this submission