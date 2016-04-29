# DATA Act Core

The DATA Act Core is a collection of common components used by other DATA Act packages.

## Background

The U.S. Department of the Treasury is building a suite of open-source tools to help federal agencies comply with the [DATA Act](http://fedspendingtransparency.github.io/about/ "Federal Spending Transparency Background") and to deliver the resulting standardized federal spending information back to agencies and to the public.

For more information about the DATA Act Broker codebase, please visit this repository's [main README](../README.md "DATA Act Broker Backend README").

## Data Broker Core Database Reference

The DATA Act broker uses the following databases; the models and setup scripts for all of them live in the DATA Act core project.

* `error_data` - Holds file level errors in the `file_status` table, along with information about number of row level errors of each type in the `error_data` table. A complete list of every separate occurrence can be found in the error report csv file.
* `job_tracker` - Holds information on all validation and upload jobs, including status of jobs and relations between jobs. The `job_status` table is the main place to get this information and provides file name/type, status of the job, the job's associated submission, and the table in which the results are located. The `job_dependency` table details precedence constraints between jobs, giving the job IDs for both the prerequisite and the dependent job.
* `staging` - Holds records that passed validation. Each file validated will have a table in this database, named based on the job ID. If the `file_status` table in the `error_data` database lists the file as completed, each record in the input file will be present in either this staging table or the error report.
* `user_manager` - Holds a mapping between user names and user IDs to be used for providing submission history information to a user.
* `validation` - Contains all the information a submitted file is validated against. The `file_columns` table details what columns are expected in each file type, and the rule table maps all defined single-field rules to one of the columns specified in `file_columns`. The `multi_field_rule` table stores rules that involve a set of fields, but are still checked against a single record at a time. Finally, the `tas_lookup` table holds the set of valid TAS combinations, taken from the TAS csv file discussed in the setup section.

## DATA Act Core Project Layout

The DATA Act Core is a collection of common components used by other
DATA Act packages.  The directory structure is as follows:

```
dataactcore/
├── aws/            (Common AWS functions)
├── credentials/    (Database credentials)
├── models/         (ORM models and database interfaces)
├── scripts/        (Database setup scripts)
└── utils/          (JSON helper objects)
```

#### AWS

The `aws/` folder contains all of the common code that uses AWS Boto SDK, which requires the AWS CLI to function correctly. The installation instructions for the AWS CLI can be found in the [DATA Act installation guide](https://github.com/fedspendingtransparency/data-act-broker-backend/blob/master/doc/INSTALL.md).

#### Models

The `models/` folder contains the object-relational mapping (ORM) definition for all models used in the DATA Act project. When a new table is needed, a new object needs to be defined using the SQLAlchemy object notation. For example, a table with a single column of
text and a primary key should be defined as follows.

```python

from sqlalchemy import Column, Integer, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
class ExampleTable(Base):
    __tablename__ = 'example_table'

    example_table_id = Column(Integer, primary_key=True)
    text_field = Column(Text)

```

Note that all new ORM objects must inherit from the `declarative_base` object and have the `__tablename__` field set. For consistency, field and tables names should be in all lower case, separated by `_` between words.

Additional fields exist on some of the models to enable the automatic population of foreign key relationships. These fields use the `relationship` function to signify a mapping to another table.  More information on SQLAlchemy ORM objects can be found on the [official website](http://docs.sqlalchemy.org/en/latest/orm/tutorial.html#create-a-schema).

Database interfaces are defined for each database used within the project. Each interface inherits base functionality from `BaseInterface` and defines both the database name and credentials file location. Where required, interfaces are extended in the other repositories to add additional functionality.

#### Scripts

The `scripts/` folder contains various python scripts used in the DATA Act broker backend install process, including database creation scripts. When run directly, the following scripts take no parameters and stand up all required tables within each database:

- setupJobTrackerDB (Creates job_tracker database)
- setupErrorDB      (Creates the error database)
- setupUserDB       (Creates the user database)
- setupAllDB        (Creates all of the needed databases)

The order of execution does not matter, as long as each of them are executed.

To clean out the databases for testing proposes, the following scripts are also provided:

- clearErrors (Clears error_data and file_status tables)
- clearJobs (Clears job_dependency, job_status, and submission tables)

These scripts should **not** be used in a live production environment, as existing queries may hold references to the deleted data.

#### Utils

The `utils/` folder contains common REST requests and error handling objects.
These provide a common way for other broker components to handle requests.

The `RequestDictionary` class is used throughout the DATA Act repositories to provide a
seamless method to access both the JSON Body and POST FormData from a REST request.
For example, if the following JSON was sent to a REST endpoint:

```json
{
  "data" : "value"
}
```

It would be accessed by:

```json

    requestDictionary = RequestDictionary(request)
    value = requestDictionary.getValue("data")

```

The `JsonResponse` object contains methods for automatically encoding a JSON response
from a REST request. Users are able to pass dictionary objects that will be
automatically converted to JSON with the correct application headers added.

In addition, the static `error` method will auto create a JSON response with the current exception stack trace encoded. This is useful in the development environment, but should be disabled in production by setting the static class variable `printDebug` to `false`.
