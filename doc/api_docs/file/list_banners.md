# GET "/v1/list\_banners/"
This endpoint returns a list of temporary messages to display on the Broker frontend as a banner.

## Sample Request
`/v1/list_banners/`

## Request Params
N/A

## Response (JSON)
```
{
    "data": [
        {
            "banner_type": "warning",
            "start_date": "2019-12-01",
            "end_date": "2019-12-09",
            "header": null,
            "notice_block": true,
            "type": "dabs",
            "message": "Submissions cannot be certified until ..."
        },
        {
            "banner_type": "info",
            "start_date": "2019-12-01",
            "end_date": "2019-12-09",
            "header": null,
            "notice_block": false,
            "type": "all",
            "message": "As a result of an issue identified ..."
        }
    ]
}
```

## Response Attributes
- `data`: ([dict]) available banner messages to display on the site
    - `header`: (string) The header of the banner.
    - `message`: (string) The message for the banner.
    - `banner_type`: (string) The type of banner. Values include:
        - `info`: for informational messages
        - `warning`: for more pressing messages
    - `type`: (string) Which pages to display the message. Values include:
        - `fabs`: only FABS pages
        - `dabs`: only DABS pages
        - `all`: all pages
    - `start_date`: (string) When to start displaying the message.
    - `end_date`: (string) The last day to display the message.
    - `notice_block`: (boolean) Whether the frontend will block submissions during the message period.