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

The file location on S3 is specified in the job tracker, and the validator streams the file record by record from S3.  The first row is the headers, which are checked against the file specification to ensure that all headers in the file are known fields and that no required fields are missing.

Each record is checked against the set of rules for the fields present in the file, with all errors and warnings recorded.  With the exception of records that fail a data type check, each record goes into a staging table in the validation database. Data that fails any validation rule is written to either an error report or warning report, and a running sum is kept of occurrences for each rule, which are written to the error database after all rows have been checked.  Finally, the job is marked as finished in the job tracker, the file is marked completed in the error database.

## Available Validations
The available rule types are as follows:

* Type checks - verifies that data fits the specified type
* Equal / Not equal - checks that value is equal to a specified value, or not equal
* Less than / Greater than - compares value against a specified reference point
* Length - checks that value is no longer than specified length, this is treated as a warning
* Set membership - checks that value is one of an allowed set of values
* Minimum length - checks that the field has at least the number of characters specified
* Conditional requirement - checks that the field is populated if another specified rule passes
* Sum - Checks that a field is equal to a sum of other fields
* Sum to value - Checks that a set of fields sums to a specified value
* Require one of - Checks that at least one of a set of fields is populated
* TAS - Check that a TAS number is present in CARS file
* Field match - Checks that a set of field values is present in another file
* Conditional rule - Checks first rule if and only if second rule passes

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

* `ErrorInterface`, `JobTrackerInterface`, `ValidationInterface` - These classes control communication with their respective databases
* `InterfaceHolder` - Container that holds one interface for each database as a static variable to ensure that redundant connections are not created

### Scripts

The `/dataactvalidator/scripts` folder contains the install scripts needed to setup the validator for a local install. For complete instructions on running your own copy of the validator and other DATA Act broker components, please refer to the [documentation in the DATA Act core responsitory](https://github.com/fedspendingtransparency/data-act-broker-backend/blob/master/doc/INSTALL.md "DATA Act broker installation guide").

## Test Cases
To run the validator unit tests, navigate to the main project's test folder (`data-act-broker-backend/tests`) and type the following:

        $ python runTests.py

To generate a test coverage report from the command line:

1. Make sure you're in the tests folder (`data-act-broker-backend/tests`).
2. Run the tests using the `coverage` command: `coverage run runTests.py`.
3. After the tests are done running, view the coverage report by typing `coverage report`. To exclude third-party libraries from the report, you can tell it to ignore the `site-packages` folder: `coverage report --omit=*/site-packages*`.
