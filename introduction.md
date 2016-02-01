## The DATA Act Core Repository

The DATA Act Core repository is a collection of common components used by other
DATA Act repositories.  The structure for the repository is as follows:

```
dataactcore/
├── aws/            (Common AWS functions)
├── credentials/    (Database credentials)
├── models/         (ORM models and database interfaces)
├── scripts/        (Database setup scripts)
└── utils/          (JSON helper objects)
```

#### AWS

The `aws/` folder contains all of the common code that uses AWS Boto SDK, which requires the AWS CLI to function correctly. The installation instructions for the AWS CLI can be found in the [DATA Act installation guide](README.md#AWSCLI).


#### Models

The `models/` folder contains the object-relational mapping (ORM) definition for all models used in the DATA Act project. When a new table is needed, a new object needs to be defined using the SQLAlchemy object notation. For example, a table with a single column of
text and a primary key should be defined as follows.

````

from sqlalchemy import Column, Integer, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
class ExampleTable(Base):
    __tablename__ = 'example_table'

    example_table_id = Column(Integer, primary_key=True)
    text_field = Column(Text)


````

Note that all new ORM objects must inherit from the `declarative_base` object and have the `__tablename__` field set. For consistency, field and tables names should be in all lower case, separated by `_` between words.

Additional fields exist on some of the models to enable the automatic population of foreign key relationships. These fields use the `relationship` function to signify a mapping to another table.  More information on SQLAlchemy ORM objects can be found on the [official website](http://docs.sqlalchemy.org/en/latest/orm/tutorial.html#create-a-schema).

Database interfaces are defined for each database used within the project. Each interface inherits base functionality from `BaseInterface` and defines both the database name and credentials file location. Where required, interfaces are extended in the other repositories to add additional functionality.


#### Scripts

The `scripts/` folder contains various python scripts to setup parts of the DATA Act Core
repository for local installation. These scripts are used by the pip install process
to provide a seamless setup. See the [DATA Act installation guide]() for more details.
If needed, these scripts can be run manually to setup an environment.

`configure.py` provides interactive command line contains prompts to set the S3 bucket JSON and database access credentials. The [DATA Act installation guide]() covers the usage of these scripts.

In addition to the JSON configuration scripts, database creation scripts are located in this folder. When run directly, the following scripts take no parameters and stand up all required tables within each database:

- setupJobTrackerDB (Creates job_tracker and user Databases)
- setupValidationDB (Creates validation databases)
- setupErrorDB      (Creates the error database)
- setupStagingDB    (Creates the staging database)
- setupAllDB        (Creates all of the needed databases)

The order of execution does not matter, as long as each of them are executed.

To clean out the databases for testing proposes, the following scripts are also provided:

- clearErrors (Clears error_data and file_status tables)
- clearJobs (Clears job_dependency, job_status, and submission tables)

These scripts should **not** be used in a live production environment, as existing queries may hold references to the deleted data.

#### Utils

The `utils/` folder contains common REST requests and error handling objects.
These provide a common way for other repositories to handle requests.

The `RequestDictionary` class is used throughout the DATA Act repositories to provide a
seamless method to access both the JSON Body and POST FormData from a REST request.
For example, if the following JSON was sent to a REST endpoint:

```
{
  "data" : "value"
}
```

It would be accessed by:

```

    requestDictionary = RequestDictionary(request)
    value = requestDictionary.getValue("data")

```

The `JsonResponse` object contains methods for automatically encoding a JSON response
from a REST request. Users are able to pass dictionary objects that will be
automatically converted to JSON with the correct application headers added.

In addition, the static `error` method will auto create a JSON response with the current exception stack trace encoded. This is useful in the development environment, but should be disabled in production by setting the static class variable `printDebug` to `false`.
