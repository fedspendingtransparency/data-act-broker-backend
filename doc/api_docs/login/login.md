# POST "/v1/login/"

**THIS ENDPOINT IS FOR LOCAL DEVELOPMENT ONLY AND CANNOT BE USED TO AUTHENTICATE INTO BROKER IN PRODUCTION**

This route checks the username and password against a credentials file. It is used solely as a workaround for developing on a local instance of the broker to bypass CAIA login. Accepts input as json or form-urlencoded, with keys "username" and "password". See `active_user` docs for details.

## Body (JSON)

```
{
    "username": "user",
    "password": "pass"
}
```

## Body Description
- `username`: (required, string) the username to log in with.
- `password`: (required, string) the password associated with this user


## Response (JSON)
```
{
    "message": "Login successful",
    "user_id": 42,
    "name": "Jill",
    "title": "Developer",
    "skip_guide": False,
    "website_admin": False,
    "affiliations": [
        {"agency_name": "Department of Labor", "permission": "writer"}
    ],
    "session_id": "ABC123"
}
```


## Response Attributes
- `message`: (string) a message indicating a successful login
- `user_id`: (integer) the ID of the user as stored in the database
- `name`: (string) the name of the user
- `title`: (string) the title associated with the user
- `skip_guide`: (boolean) whether to show the DABS submission guide on the frontend or not (true = don't show)
- `website_admin`: (boolean) whether the user is a website admin or not
- `affiliations`: ([dict]) dictionaries containing information about the user's agency affiliations (only used by the frontend if `website_admin` is false). Dictionaries contain the following information:
    - `agency_name`: (string) the name of the agency the user is affiliated with
    - `permission`: (string) the level of permissions the user has. For more information about what the levels mean, see the [permissions.md](../../permissions.md) file. Possible levels:
        - `reader`
        - `writer`
        - `submitter`
        - `editfabs`
        - `fabs`
- `session_id`: (string) a hash the application uses to verify that user sending the request is logged in


## Errors
Possible HTTP Status Codes:

- 400: Missing parameters
- 401: Invalid username or password