# GET "/v1/list\_submission\_users/"
This endpoint lists all users with submissions that the requesting user can view, sorted by user name.

## Sample Request
`/v1/list_submission_users/?d2_submission=False`

## Request Params
- `d2_submission`: (boolean) if the submissions checked should be FABS or DABS (True for FABS). Defaults to `False` if not provided.

## Response (JSON)

```json
{
    "users": [
        {
            "user_id": 4,
            "name": "Another User",
            "email": "another_user@domain.com"
        },
        {
            "user_id": 1,
            "name": "User One",
            "email": "user1@domain.com"
        }
    ]
}
```

## Response Attributes

- `users`: ([dict]) contain the user's ID, name, and email:
    - `user_id`: (integer) ID of the user
    - `name`: (string) name of the user
    - `email`: (string) email of the user

## Errors
Possible HTTP Status Codes:

- 401: Login required