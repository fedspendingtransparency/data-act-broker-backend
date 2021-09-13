# GET "/v1/get\_submitted\_published\_file/"
Get a signed url for a specified submitted published file. Note, this is different from
[get\_certified\_file](./get_certified_file.md) in two ways:
* `get_certified_file` requires a submission and access to said permission. This only needs the `published_files_history_id`.
* `get_certified_file` can return all published/certified files, including warnings. This only returns submitted files, excluding warnings. 

## Sample Request
`/v1/get_submitted_published_file/?published_files_history_id=7`

## Request Params

- `published_files_history_id`: (required, integer) the `published_files_history_id` of the file 
  (obtained through [list\_history](./list_history.md) or [list\_latest_published\_files](./list_latest_published_files.md))

## Response (JSON)
```
{
    "url": "https://........",
}
```

## Response Attributes
- `url`: (string) the url to the certified

## Errors
Possible HTTP Status Codes:

- 400:
    - invalid `published_files_history_id`
    - Missing required parameter
    - User isn't logged in
