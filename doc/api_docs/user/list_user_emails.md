# GET "/v1/list\_user\_emails/"
This endpoint lists the emails of users affiliated with the requester's agencies
    
## Sample Request
`/v1/list_user_emails/`
    
## Request Params
N/A
    
## Response (JSON)
```
{
    "users": [
        {
            "id": 1,
            "name": "User 1",
            "email": "user.name@domain.com"
        },
        {
            "id": 5,
            "name": "Another User",
            "email": "another.user@domain.com"
        }
    ]
}
```
    
## Response Attributes
- `users`: ([dict]) the list of users and emails that the requester has access to. Contents of the dictionaries are:
    - `id`: (integer) the ID of the user as stored in the database
    - `name`: (string) the name of the user
    - `email`: (string) the email of the user
    
## Errors
Possible HTTP Status Codes:
    
- 401: Login required