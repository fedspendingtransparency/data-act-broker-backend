# GET "/v1/list\_history/"
Lists all the publication and certification history for a submission along with the files associated with them.

## Sample Request
`/v1/list_history/?submission_id=12345`

## Request Params
- `submission_id`: (required, integer) The ID of the submission to get publication/certification history for

## Response (JSON)
```
{
    "submission_id": 7,
    "publications": [{
        "publish_date": "2017-05-11 18:10:18",
        "publishing_user": {
            "name": "User Name",
            "user_id": 1
        },
        "published_files": [{
                "published_files_history_id": 1,
                "filename": "1492041855_file_c.csv",
                "is_warning": False,
                "comment": "Comment on the file"
            },
            {
                "published_files_history_id": 1,
                "filename": "submission_7_File_C_award_financial_warning_report.csv",
                "is_warning": True,
                "comment": None
            }
        ]},
        {
            "publish_date": "2017-05-08 12:07:18",
            "publishing_user": {
                "name": "Admin User Name",
                "user_id": 2
            },
            "published_files": [
                {
                    "published_files_history_id": 3,
                    "filename": "1492041855_file_a.csv",
                    "is_warning": False,
                    "comment": "This is also a comment"
                },
                {
                    "published_files_history_id": 6,
                    "filename": "submission_280_crossfile_warning_File_A_to_B_appropriations_program_activity.csv",
                    "is_warning": True,
                    "comment": None
                }
            ]
        }],
    "certifications": [{
        "certify_date": "2017-05-11 18:10:18",
        "certifying_user": {
            "name": "User Name",
            "user_id": 1
        },
        "certified_files": [
            {
                "published_files_history_id": 1,
                "filename": "1492041855_file_c.csv",
                "is_warning": False,
                "comment": "Comment on the file"
            },
            {
                "published_files_history_id": 1,
                "filename": "submission_7_File_C_award_financial_warning_report.csv",
                "is_warning": True,
                "comment": None
            }
        ]
    }]
}
```

## Response Attributes
- `submission_id `: (integer) the ID of the submission
- `publications`: ([dict]) Each dictionary contains the following values and represents one publication:
    - `publish_date`: (string) the date of the publication
    - `publishing_user`: (dict) contains the following details about the user that published the submission:
        - `name`: (string) the user's name
        - `user_id`: (integer) the ID of the user in the database
    - `published_files`: ([dict]) Each dictionary holds each of the published files in the submission with the following information:
        - `published_files_history_id`: (integer) the ID of the file in the `published_files_history` table, used to download the file
        - `filename`: (string) the name of the file
        - `is_warning`: (boolean) whether the file is the warning file associated with that file or the file itself
        - `comment`: (string) the comment associated with the file
- `certifications`: ([dict]) Each dictionary contains the following values and represents one certification:
    - `certify_date`: (string) the date of the certification
    - `certifying_user`: (dict) contains the following details about the user that certified the submission:
        - `name`: (string) the user's name
        - `user_id`: (integer) the ID of the user in the database
    - `certified_files`: ([dict]) Each dictionary holds each of the certified files in the submission with the following information:
        - `published_files_history_id`: (integer) the ID of the file in the `published_files_history` table, used to download the file
        - `filename`: (string) the name of the file
        - `is_warning`: (boolean) whether the file is the warning file associated with that file or the file itself
        - `comment`: (string) the comment associated with the file

## Errors
Possible HTTP Status Codes:

- 400:
    - `submission_id` missing or invalid
    - `submission_id` provided is for a FABS submission
    - User doesn't have permissions for this agency
    - Submission has no publication history (has never been published)