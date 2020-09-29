# POST "/v1/set\_skip\_guide/"
Sets whether the current user sees the DABS submission guide when creating a new DABS submission or not.

## Body (JSON)
```
{
    "skip_guide": True
}
```
    
## Body Description
- `skip_guide`: (required, boolean) Determines if the user should see the DABS submission guide when creating a new DABS submission (true = does not see).
    
## Response (JSON)
```
{
    "message": "skip_guide set successfully",
    "skip_guide": True
}
```
    
## Response Attributes
- `message`: (string) a message indicating that the `skip_guide` was set successfully
- `skip_guide`: (boolean) what the current state of `skip_guide` is
    
## Errors
Possible HTTP Status Codes:

- 400:
    - Invalid parameter
    - Missing parameter
- 401: Login required