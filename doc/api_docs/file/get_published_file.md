# GET "/v1/get\published\_file/"
Get a signed url for a specified history item

## Sample Request
`/v1/get_published_file/?submission_id=1&published_files_history_id=7&is_warning=true`

## Request Params

- `submission_id`: (required, integer) the submission ID
- `published_files_history_id`: (required, integer) the `published_files_history_id` of the file (obtained through [list\_history](./list_history.md))
- `is_warning`: (boolean) whether the file being obtained is a warning file or the file that was published. True = warning file. Default is False

## Response (JSON)
```
{
    "url": "https://........",
}
```

## Response Attributes
- `url`: (string) the url to the published file

## Errors
Possible HTTP Status Codes:

- 400:
    - invalid `published_files_history_id`
    - Missing required parameter
    - User doesn't have permissions for this agency
    - `published_files_history_id` does not match the `submission_id` provided
    - The file type requested (warning or non-warning) doesn't exist for the requested ID