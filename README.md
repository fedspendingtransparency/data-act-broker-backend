# DATA Act Validator

The validator is used to check files submitted by users against a set of rules specified in a CSV file, to ensure that data submitted is correctly formatted with reasonable values, and is not inconsistent with other sources.

## Installation

For instructions on contributing to this project or running your own copy of the DATA Act broker, please refer to the [documentation in the DATA Act core responsitory](https://github.com/fedspendingtransparency/data-act-core/blob/master/doc/INSTALL.md "DATA Act broker installation guide").

## Validator API Routes

Generally, the validator should be accessed through the broker API, which is documented on [that project's README file](https://github.com/fedspendingtransparency/data-act-broker/blob/master/README.md "DATA Act broker API README"). However, the available routes are documented below.

##### GET `/`
This is used to confirm that the validator is running

Example input: None  
Example output: `Validator is running`

##### POST `/validate/`
Called to apply validation rules to a specified job ID.  Expects a JSON with key "job_id" specifying which job to validate.  Valid records will be written to the staging database, and errors will be listed in the error report and summarized in the error database.

Example input: `{"job_id":3664}`  
Example output: None (status 200 if successful)

##### POST `/validate_threaded/`
Same as validate route, but launches validation in a separate thread and immediately returns 200.  Success or failure of the validation should then be determined from the job tracker database.  

Example input: `{"job_id":3664}`  
Example output: None

## Process Overview
The validation process begins with a call to one of the above routes from either the job manager or the broker API, specifying the job ID for the validation job.  First, the validator checks the job tracker to ensure that the job is of the correct type, and that all prerequisites are completed.

The file location on S3 is specified in the job tracker, and the validator streams the file record by record from S3.  The first row is the headers, which are checked against the file specification to ensure that all headers in the file are known fields and that no required fields are missing.

Each record is checked against the set of rules for the fields present in the file.  If a record passes all rules, it goes into the staging database.  Otherwise, each failed rule goes into the error report, and a running sum is kept of error occurrences for each rule, which are written to the error database after all rows have been checked.  Finally, the job is marked as finished in the job tracker, the file is marked completed in the error database.

## Available Validations
The available rule types are as follows:

* Type checks - verifies that data fits the specified type
* Equal / Not equal - checks that value is equal to a specified value, or not equal
* Less than / Greater than - compares value against a specified reference point
* Length - checks that value is no longer than specified length
* Set membership - checks that value is one of an allowed set of values
* Minimum length - checks that the field has at least the number of characters specified
* Conditional requirement - checks that the field is populated if another specified rule passes

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

* `ErrorInterface`, `JobTrackerInterface`, `StagingInterface`, `ValidationInterface` - These classes control communication with their respective databases
* `StagingTable` - Used to create a new table for each job and manage writes to that table
* `InterfaceHolder` - Container that holds one interface for each database as a static variable to ensure that redundant connections are not created

### Models
* `validationModels` - Holds all the ORM objects for the validation database

### Scripts

The `/dataactvalidator/scripts` folder contains the install scripts needed to setup the validator for a local install. For complete instructions on running your own copy of the validator and other DATA Act broker components, please refer to the [documentation in the DATA Act core responsitory](https://github.com/fedspendingtransparency/data-act-core/blob/master/doc/INSTALL.md "DATA Act broker installation guide").

## Test Cases
To run the validator unit tests, navigate to the main project's test folder (`data-act-validator/tests`) and type the following:

        $ python runTests.py

To generate a test coverage report from the command line:

1. Make sure you're in the tests folder (`data-act-validator/tests`).
2. Run the tests using the `coverage` command: `coverage run runTests.py`.
3. After the tests are done running, view the coverage report by typing `coverage report`. To exclude third-party libraries from the report, you can tell it to ignore the `site-packages` folder: `coverage report --omit=*/site-packages*`.
