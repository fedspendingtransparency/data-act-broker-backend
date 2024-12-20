# The DATA Broker Application Programming Interface (API)

The DATA Broker API powers the Data Broker's data submission process.

## Background

The U.S. Department of the Treasury is building a suite of open-source tools to help federal agencies comply with the [DATA Act](http://fedspendingtransparency.github.io/about/ "Federal Spending Transparency Background") and to deliver the resulting standardized federal spending information back to agencies and to the public.

For more information about the Data Broker codebase, please visit this repository's [main README](../README.md "Data Broker Backend README").

**A Note on CGAC/FREC**: In the vast majority of cases, top-level agencies identify themselves for purposes of DABS submissions or detached D1/D2 file generation by their 3-digit CGAC code. CGAC are issued and managed by OMB and are updated yearly in the A-11 circular appendix C. The CGAC is equivalent to the treasury concept of the Agency Identifier (AID) embedded in all Treasury Account Symbols (TAS).

In a few cases, legitimately separate (at least for financial reporting purposes) agencies share a CGAC. To allow them to report as separate entities in the Data Broker, we leveraged an internal Treasury element called the Financial Reporting Entity Code (FREC) that Treasury already uses to distinguish between these agencies with shared AID at the TAS level. This field comes from Treasury's CARS system.
These agencies, listed in the table below, should use this four-digit FREC code for purposes of identifying themselves in DABS instead of the CGAC they share with one or more agencies.
The following is the complete list of agencies supported under the FREC paradigm in DABS. These agencies should always identify themselves to the Broker with the 4-digit FREC code instead of the 3 digit CGAC they share with other agencies.

|SHARED CGAC|	AGENCY NAME|AGENCY ABBREVIATION|Financial Reporting Entity Code (FREC)|
|-----------|-----------|---------------------|--------------------------------------|
|011|EOP Office of Administration|EOPOA|1100|
|011|Peace Corps|Peace Corps|1125|
|011|Inter-American Foundation|IAF|1130|
|011|U.S. Trade and Development Agency|USTDA|1133|
|011|African Development Foundation|ADF|1136
|016|Department of Labor|DOL|1601|
|016|Pension Benefit Guaranty Corporation|PBGC|1602|
|033|Smithsonian Institution|SI|3300|
|033|John F. Kennedy Center For The Performing Arts|Kennedy Center|3301|
|033|National Gallery of Art|	National Gallery|3302|
|033|Woodrow Wilson International Center For Scholars|Wilson Center|3303|
|352|Farm Credit Administration|FCA|7801|
|352|Farm Credit System Insurance Corporation|FCSIC|7802|
|537|Federal Housing Finance Agency|FHFA|9566|
|537|Federal Housing Finance Agency Inspector General|FHFAIG|9573|


## Broker API Project Layout

The Broker API has two major directories: scripts and handlers.

```
dataactbroker/
├── scripts/        (Install and setup scripts)
└── handlers/       (Route handlers)
```

### Scripts
The `/dataactbroker/scripts` folder contains the install scripts needed to setup the broker API for a local install. For complete instructions on running your own copy of the API and other Data Broker components, please refer to the [documentation in the Data Broker core repository](https://github.com/fedspendingtransparency/data-act-broker-backend/blob/master/doc/INSTALL.md "Data Broker installation guide").

### Handlers
The `dataactbroker/handlers` folder contains the logic to handle requests that are dispatched from various routes files. Routes defined in these files may include the `@requires_login` and `@requires_submission_perms` tags to the route definition. This tag adds a wrapper that checks if there exists a session for the current user and if the user is logged in, as well as checking the user's permissions to determine if the user has access to this route. If user is not logged in to the system or does not have access to the route, a 401 HTTP error will be returned. This tags are defined in `dataactbroker/permissions.py`.

`account_handler.py` contains the functions to check logins and to log users out.

`fileHandler.py` contains functions for managing user file interaction. It creates all of the jobs that are part of the user submission and has query methods to get the status of a submission. In addition, this class creates downloadable links to error reports created by the Data Broker Validator.

## Data Broker Route Documentation

The base URL for all routes is `https://broker-api.usaspending.gov/`. This should be present ahead of all listed routes in order to use the Broker API (e.g. `https://broker-api.usaspending.gov/list_agencies`).

All routes that require a login should be passed a header "x-session-id". The value for this header should be taken from the login route response header "x-session-id".

Route documentation is split into folders that reflect the internal organization of routes within the codebase. Each of these sections is detailed below, including a general description of the kinds of routes in each folder and links to documentation for each route in the folder.

**All routes have an optional trailing slash, meaning the route will work with or without it.**

### GET "/"
This route is simply a check to ensure that Broker is running.

Response: "Broker is running"

### Dashboard Routes
Dashboard routes are primarily used by the DABS dashboard for analytical displays of warning/error data within a submission or set of submissions.

- [active\_submission\_overview](./api_docs/dashboard/active_submission_overview.md)
- [active\_submission\_table](./api_docs/dashboard/active_submission_table.md)
- [get\_impact\_counts](./api_docs/dashboard/get_impact_counts.md)
- [get\_rule\_labels](./api_docs/dashboard/get_rule_labels.md)
- [get\_significance\_counts](./api_docs/dashboard/get_significance_counts.md)
- [historic\_dabs\_graphs](./api_docs/dashboard/historic_dabs_graphs.md)
- [historic\_dabs\_table](./api_docs/dashboard/historic_dabs_table.md)

### Domain Routes
Domain routes are primarily used to provide domain information, such as lists of CGAC/FREC agencies.

- [list\_agencies](./api_docs/domain/list_agencies.md)
- [list\_all\_agencies](./api_docs/domain/list_all_agencies.md)
- [list\_data\_sources](./api_docs/domain/list_data_sources.md)
- [list\_sub\_tier\_agencies](./api_docs/domain/list_sub_tier_agencies.md)

### File Routes
File routes are primarily routes that are directly related to portions of submissions that are not covered by other subsets of routes, such as the generation routes section.

- [certify\_dabs\_submission](./api_docs/file/certify_dabs_submission.md)
- [check\_current\_page](./api_docs/file/check_current_page.md)
- [check\_status](./api_docs/file/check_status.md)
- [delete\_submission](./api_docs/file/delete_submission.md)
- [get\_published\_file](./api_docs/file/get_published_file.md)
- [get\_submitted\_published\_file](./api_docs/file/get_submitted_published_file.md)
- [get\_comments\_file](./api_docs/file/get_comments_file.md)
- [get\_detached\_file\_url](./api_docs/file/get_detached_file_url.md)
- [get\_fabs\_meta](./api_docs/file/get_fabs_meta.md)
- [get\_file\_url](./api_docs/file/get_file_url.md)
- [get\_obligations](./api_docs/file/get_obligations.md)
- [get\_submission\_comments](./api_docs/file/get_submission_comments.md)
- [get\_submission\_zip](./api_docs/file/get_submission_zip.md)
- [latest\_publication\_period](./api_docs/file/latest_publication_period.md)
- [list\_banners](./api_docs/file/list_banners.md)
- [list\_history](./api_docs/file/list_history.md)
- [list\_submissions](./api_docs/file/list_submissions.md)
- [list\_latest\_published\_files](./api_docs/file/list_latest_published_files.md)
- [publish\_and\_certify\_dabs\_submission](./api_docs/file/publish_and_certify_dabs_submission.md)
- [publish\_dabs\_submission](./api_docs/file/publish_dabs_submission.md)
- [publish\_fabs\_file](./api_docs/file/publish_fabs_file.md)
- [published\_submissions](./api_docs/file/published_submissions.md)
- [report\_url](./api_docs/file/report_url.md)
- [restart\_validation](./api_docs/file/restart_validation.md)
- [revalidation\_threshold](./api_docs/file/revalidation_threshold.md)
- [revert\_submission](./api_docs/file/revert_submission.md)
- [submission\_data](./api_docs/file/submission_data.md)
- [submission\_metadata](./api_docs/file/submission_metadata.md)
- [update\_submission\_comments](./api_docs/file/update_submission_comments.md)
- [upload\_dabs\_files](./api_docs/file/upload_dabs_files.md)
- [upload\_fabs\_file](./api_docs/file/upload_fabs_file.md)

### Generation Routes
Generation routes are used for generating files (A, D1, D2, E, and F) and checking on the status of generations that have been started.

- [check\_detached\_generation\_status](./api_docs/generation/check_detached_generation_status.md)
- [check\_generation\_status](./api_docs/generation/check_generation_status.md)
- [generate\_detached\_file](./api_docs/generation/generate_detached_file.md)
- [generate\_file](./api_docs/generation/generate_file.md)

### Login Routes
Login routes are used to log a user in or out or check if the current session is still active.

- [login](./api_docs/login/login.md)
- [logout](./api_docs/login/logout.md)
- [caia\_login](./api_docs/login/caia_login.md)
- [session](./api_docs/login/session.md)

### Settings Routes
Settings routes are used to set agency-wide settings for the DABS Dashboard. These only affect the dashboard display and will have no bearing on actual submissions.

- [rule\_settings](./api_docs/settings/rule_settings.md)
- [save\_rule\_settings](./api_docs/settings/save_rule_settings.md)

### User Routes
User routes are used to get information about available users or get/set information about the currently logged in user.

- [active\_user](./api_docs/user/active_user.md)
- [list\_submission\_users](./api_docs/user/list_submission_users.md)
- [list\_user\_emails](./api_docs/user/list_user_emails.md)
- [set\_skip\_guide](./api_docs/user/set_skip_guide.md)

## Automated Tests

Many of the broker tests involve interaction with a test database. However, these test databases are all created and 
torn down dynamically by the test framework, as new and isolated databases, so a live PostgreSQL server is all that's
needed.

These types of tests _should_ all be filed under the `data-act-broker-backend/tests/integration` folder, however the 
reality is that many of the tests filed under `data-act-broker-backend/tests/unit` also interact with a database. 

So first, ensure your `dataactcore/local_config.yml` and `dataactcore/local_secrets.yml` files are configured to be 
able to connect and authenticate to your local Postgres database server as instructed in [INSTALL.md](../doc/INSTALL.md) 

**To run _all_ tests**
```bash
$ pytest
```

**To run just _integration_ tests**
```bash
$ pytest tests/integration/*
```

**To run just _Broker API_ unit tests**
```bash
$ pytest tests/unit/dataactbroker/*
```

To generate a test coverage report with the run, just append the `--cov` flag to the `pytest` command.
