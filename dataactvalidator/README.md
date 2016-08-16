# DATA Act Validator

The DATA Act validator checks submitted DATA Act files against a set of rules to ensure that data submitted is correctly formatted with reasonable values and is not inconsistent with other sources.

## Background

The U.S. Department of the Treasury is building a suite of open-source tools to help federal agencies comply with the [DATA Act](http://fedspendingtransparency.github.io/about/ "Federal Spending Transparency Background") and to deliver the resulting standardized federal spending information back to agencies and to the public.

For more information about the DATA Act Broker codebase, please visit this repository's [main README](../README.md "DATA Act Broker Backend README").

## Validator API Routes

Generally, the validator should be accessed through the broker API, which is documented on [that project's README file](https://github.com/fedspendingtransparency/data-act-broker-backend/blob/master/README.md "DATA Act broker API README"). However, the available routes are documented below.

##### GET `/`
This is used to confirm that the validator is running

Example input: None
Example output: `Validator is running`

##### POST `/validate/`
Called to apply validation rules to a specified job ID.  Expects a JSON with key "job_id" specifying which job to validate.  Records will be written to a set of tables in the validation database, and errors will be listed in the error report and summarized in the error database.

Example input: `{"job_id":3664}`
Example output: None (status 200 if successful)

##### POST `/validate_threaded/`
Same as validate route, but launches validation in a separate thread and immediately returns 200.  Success or failure of the validation should then be determined from the job tracker database.

Example input: `{"job_id":3664}`
Example output: None

## Process Overview
The validation process begins with a call to one of the above routes from either the job manager or the broker API, specifying the job ID for the validation job.  First, the validator checks the job tracker to ensure that the job is of the correct type, and that all prerequisites are completed.

The file location on S3 is specified in the job tracker, and the validator streams the file record by record from S3.

The validation process for each submitted group of files happens in four steps:

1. Each individual file is checked for correct formatting and a correct header row.
2. Basic schema checks are performed on each row of each individual file:
    * are required fields present?
    * is the data type of each field correct?
    * is the field length correct? (warning)
3. The complex validation rules are performed on each individual file.
4. Once the individual files have passed the previous validation steps, the validator runs a series of "cross-file" checks to ensure that data is consistent between the files.

Finally, the job is marked as finished in the job table, and the file is marked completed in the error metadata table.

Some of the validation rules are defined as `errors` and some as `warnings`. Any data that fails a rule marked as `error` will result in a failed validation status, and users will have to fix the problematic data and re-submit.

All warnings and errors are displayed on the data broker's website, and their details are written to error and warning reports that are available for download.

## Validation details

The basic schema checks (number 2, above) are defined as part of the detailed DATA Act schema.

**Note:** Any data that fails the basic schema checks will result in a validation error. The exception is field length check, which will result in a warning.

The complex rule validations (including both individual file and cross-file rules) are written in SQL and can be viewed here: [config/sqlrules/](config/sqlrules/ "SQL validation rules").

[This file](config/sqlrules/sqlRules.csv "SQL validation rules overview") provides an overview of the SQL-based rules, including their corresponding error messages and whether or not each will produce an error or a warning.

## Class Descriptions

### Validation Handlers

* `ValidationError` - Enumeration of error types and related messages
* `ValidationManager` - Outer level manager that runs the validations, checking all relevant rules against each record.
* `Validator` - Checks all rules against a single record

### Filestreaming

* `CsvReader` - Streams CSV files from S3, returning one record at a time
* `CsvWriter` - Streams error report up to S3, one record at a time
* `SchemaLoader` - Reads a specification of fields and rules to be applied to specified file type.
* `TASLoader` - Loads valid TAS combinations from CARS file

### Interfaces

* `ErrorInterface`, `JobTrackerInterface`, `ValidationInterface` - These classes control communication with the database. They are designed to separate database interactions by logical function.
* `InterfaceHolder` - Container that holds one interface for each database as a static variable to ensure that redundant connections are not created

### Scripts

The `/dataactvalidator/scripts` folder contains the install scripts needed to setup the validator for a local install. For complete instructions on running your own copy of the validator and other DATA Act broker components, please refer to the [documentation in the DATA Act core responsitory](https://github.com/fedspendingtransparency/data-act-broker-backend/blob/master/doc/INSTALL.md "DATA Act broker installation guide").

## Test Cases

### Integration Tests
To run the validator integration tests, navigate to the main project's test folder (`data-act-broker-backend/tests`) and type the following:

        $ python integration/runTests.py

To generate a test coverage report from the command line:

1. Make sure you're in the tests folder (`data-act-broker-backend/tests`).
2. Run the tests using the `coverage` command: `coverage run integration/runTests.py`.
3. After the tests are done running, view the coverage report by typing `coverage report`. To exclude third-party libraries from the report, you can tell it to ignore the `site-packages` folder: `coverage report --omit=*/site-packages*`.
