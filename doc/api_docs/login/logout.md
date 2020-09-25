# POST "/v1/logout/"
Logs the current user out, only the login route will be accessible until the next login.

## Body (JSON)
N/A
    
## Body Description
N/A

## Response (JSON)
```
{
    "message": "Logout successful"
}
```
    
## Response Attributes
- `message`: (string) a message indicating a successful logout
    
## Errors
N/A