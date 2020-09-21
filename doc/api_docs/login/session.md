# GET "/v1/session/"
Checks that the session is still valid.

## Sample Request
`/v1/session/`
    
## Request Params
N/A
    
## Response (JSON)
```
{
    "status": "True"
}
```
    
## Response Attributes
- `status`: (string) whether the session is still active or not. True if active, False if inactive.

    
## Errors
N/A