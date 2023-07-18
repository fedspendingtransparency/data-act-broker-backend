# POST "/v1/max\_login/"
This route sends a request to the backend with the ticket obtained from the MAX login endpoint in order to verify authentication and access to the Data Broker. If called by a service account, a certificate is required for authentication. **IMPORTANT**: The ticket has a 30 second expiration window so it must be used immediately after being received in order for it to be valid.

### **NOTE**: This endpoint is deprecated, please use `caia_login` instead.

## Body (JSON)

```
{
    "ticket": "ST-123456-abcdefghijklmnopqrst-login.max.gov",
    "service": "https://broker-api.usaspending.gov"
}
```

## Body Description

- `ticket`: (required, string) ticket string received from MAX from initial login request (pending validation)
- `service`: (required, string) URL encoded string that is the source of the initial login request. This may vary from the example based on the environment you are in.

## Response (JSON)
More data will be added to the response depending on what we get back from MAX upon validating the ticket.

```
{
    "user_id": 42,
    "name": "John",
    "title": "Developer",
    "skip_guide": false,
    "website_admin": false,
    "affiliations": [
        {
            "agency_name": "Department of Labor (DOL)",
            "permission": "writer"
        }
    ],
    "session_id": "ABC123",
    "message": "Login successful"
}
```

##### Response Description:
- `user_id`: (integer) database identifier of the logged in user, part of response only if login is successful
- `name`: (string) user's name, part of response only if login is successful
- `title`: (string) title of user, part of response only if login is successful
- `skip_guide`: (boolean) whether to show the DABS submission guide on the frontend or not (true = don't show), part of response only if login is successful
- `website_admin`: (boolean) describes a super-user status, part of response only if login is successful
- `affiliations`: ([dict]) dictionaries containing information about the user's agency affiliations (only used by the frontend if `website_admin` is false). Dictionaries contain the following information:
    - `agency_name`: (string) the name of the agency the user is affiliated with
    - `permission`: (string) the level of permissions the user has. For more information about what the levels mean, see the [permissions.md](../../permissions.md) file. Possible levels:
        - `reader`
        - `writer`
        - `submitter`
        - `editfabs`
        - `fabs`
- `message`: (string) login error response "You have failed to login successfully with MAX", otherwise says "Login successful"
- `errorType`: (string) type of error, part of response only if login is unsuccessful
- `session_id`: (string) a hash the application uses to verify that user sending the request is logged in, part of response only if login is successful


## Errors
Possible HTTP Status Codes:

- 400: Missing parameters
- 401: Login denied