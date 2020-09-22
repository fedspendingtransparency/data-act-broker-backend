# GET "/v1/current\_user/"
Gets the information of the current that is logged in to the system.

## Sample Request
`/v1/current_user/`
    
## Request Params
N/A
    
## Response (JSON)
```
{
    "user_id": 42,
    "name": "John Doe",
    "title": "Developer",
    "skip_guide": False,
    "website_admin": False,
    "affiliations": [
        {"agency_name": "Department of Labor", "permission": "writer"}
    ],
    "session_id": "ABCD-EFGH"
}
```
    
## Response Attributes
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
    
- 401: Requires login