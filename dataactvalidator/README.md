# DATA Act Validator

The DATA Act validator checks submitted DATA Act files against a set of rules to ensure that data submitted is correctly formatted with reasonable values and is not inconsistent with other sources.

## Background

The U.S. Department of the Treasury is building a suite of open-source tools to help federal agencies comply with the [DATA Act](http://fedspendingtransparency.github.io/about/ "Federal Spending Transparency Background") and to deliver the resulting standardized federal spending information back to agencies and to the public.

For more information about the DATA Act Broker codebase, please visit this repository's [main README](../README.md "DATA Act Broker Backend README").

## Process Overview
The validation process begins with a job ID being pushed to the job manager, an AWS SQS queue. The validator is constantly polling the aforementioned queue, and when it receives a message (the job ID), it kicks of the validation process. First, the validator checks the job tracker to ensure that the job is of the correct type, and that all prerequisites are completed.

The file location on S3 is specified in the job tracker, and the validator downloads it from S3 to a temporary file.

The validation process for each submitted group of files happens in four steps:

1. Each individual file is checked for a correct header row, rows with too many/few fields, and retrieves an initial row count.
2. The file is then broken down into batches (10k rows) and the following points operate on each batch.
    *. Basic schema checks are performed on each row of each batch:
        * are required fields present?
        * is the data type of each field correct? (rows with these errors will then be ignored by any other validation)
        * is the field length correct?
        * is the data format appropriate for its data type?
    *. The data from each batch is then loaded into the staging tables.
3. SQL validation rules are performed on the data loaded into the staging tables:
    * These encompass the business logic laid out in the DAIMS Schema rules.
    * A list of all these rules can be found in [sqlRules.csv](config/sqlrules/sqlRules.csv)
4. For DABS submissions, once the individual files have passed the previous validation steps for A/B/C/D1/D2, the validator runs a series of "cross-file" checks to ensure that data is consistent between the files.
    * These are also executed via SQL and listed in [sqlRules.csv](config/sqlrules/sqlRules.csv)

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


### Scripts

The `/dataactvalidator/scripts` folder contains the install scripts needed to setup the validator for a local install. For complete instructions on running your own copy of the validator and other DATA Act broker components, please refer to the [documentation in the DATA Act core repository](https://github.com/fedspendingtransparency/data-act-broker-backend/blob/master/doc/INSTALL.md "DATA Act broker installation guide").

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

**To run just _Broker Validator_ unit tests**
```bash
$ pytest tests/unit/dataactvalidator/*
```

To generate a test coverage report with the run, just append the `--cov` flag to the `pytest` command.
