# Data Act Validator

The validator is used to check files submitted by users against a set of rules specified in a CSV file, to ensure that data submitted is correctly formatted with reasonable values, and is not inconsistent with other sources.

#### Route Usage
Generally, the validator should be accessed through the broker API, documented in data-act-broker.  However, the available routes are documented below.

##### GET `/`
This is used to confirm that the validator is running

Example input: None  
Example output: `Validator is running`

##### POST `/validate/`
Called to apply validation rules to a specified job ID.  Valid records will be written to the staging database, and errors will be listed in the error report and summarized in the error database.

Example input: `{"job_id":3664}`
Example output: None (status 200 if successful)

##### POST `/validate_threaded/`
Same as validate route, but launches validation in a separate thread and immediately returns 200.  Success or failure of the validation should then be determined from the job tracker database.  

Example input: `{"job_id":3664}` 
Example output: None

#### Process Overview
The validation process begins with a call to one of the above routes from either the job manager or the broker API, specifying the job ID for the validation job.  First, the validator checks the job tracker to ensure that the job is of the correct type, and that all prerequisites are completed.

The file location on S3 is specified in the job tracker, and the validator streams the file record by record from S3.  The first row is the headers, which are checked against the file specification to ensure that all headers in the file are known fields and that no required fields are missing.

Each record is checked against the set of rules for the fields present in the file.  If a record passes all rules, it goes into the staging database.  Otherwise, each failed rule goes into the error report, and a running sum is kept of error occurrences for each rule, which are written to the error database after all rows have been checked.  Finally, the job is marked as finished in the job tracker, the file is marked completed in the error database.

#### Available Validations
The available rule types are as follows:
* Type checks - verifies that data fits the specified type
* Equality - checks that values is equal to a specified value.  Not equal is also available.
* Less than / Greater than - compares value against a specified reference point
* Length - checks that value is no longer than specified length
* Set membership - checks that value is one of an allowed set of values  

#### Class Descriptions

##### Validation Handlers

* `ValidationError` - Enumeration of error types and related messages
* `ValidationManager` - Outer level manager that runs the validations, checking all relevant rules against each record.
* `Validator` - Checks all rules against a single record

##### Filestreaming

* `CsvReader` - Streams CSV files from S3, returning one record at a time
* `CsvWriter` - Streams error report up to S3, one record at a time
* `SchemaLoader` - Reads a specification of fields and rules to be applied to specified file type.
* `TASLoader` - Loads valid TAS combinations from CARS file

##### Interfaces

* `ErrorInterface`, `JobTrackerInterface`, `StagingInterface`, `ValidationInterface` - These classes control communication with their respective databases
* `StagingTable` - Used to create a new table for each job and manage writes to that table
* `InterfaceHolder` - Container that holds one interface for each database as a static variable to ensure that redundant connections are not created

#### Test cases
The current test suite for the validator may be run by navigating to the tests folder and running python runTests.py

# Installation

## Requirements

Data Act Validator is currently being built with Python 2.7.

## Install dependencies

### Python 2.7, `pip`, and AWS CLI Tools

Instructions for installing all three of these tools on your system can be found [here](https://docs.aws.amazon.com/cli/latest/userguide/installing.html#install-with-pip).

### Homebrew (Mac OS X)

We recommend using [homebrew](http://brew.sh) to install PostgreSQL for development on Mac OS X.

### PostgreSQL

[PostgreSQL](https://en.wikipedia.org/wiki/PostgreSQL) is an object-relational database management system (ORDBMS) with an emphasis on extensibility and standards-compliance, and is our database of choice for performing validations.

```bash
# Ubuntu/Linux 64-bit
$ sudo apt-get install postgresql postgresql-contrib

# Mac OS X
$ brew install postgresql
```

Upon install, follow the provided instructions to start postgres locally.

## Installing the Validator

Install the validator and its dependencies with 'pip':

```bash
$ sudo pip install --process-dependency-links git+git://github.com/fedspendingtransparency/data-act-validator.git@configuration
```

Note: we recommend [virtualenv](https://virtualenv.readthedocs.org/en/latest/installation.html) and [virtualenvwrapper](https://virtualenvwrapper.readthedocs.org/en/latest/install.html) to manage Python environments.

Then, configure AWS using the CLI tools you installed earlier:

```bash
$ aws configure
// Enter your Access Key ID, Secret Access Key and region
```

Initialize the validator and follow the prompted steps:

```bash
$ sudo validator –initialize
```

Finally, once the validator has been initialized, run the validator:

```bash
$ sudo validator -start
```
