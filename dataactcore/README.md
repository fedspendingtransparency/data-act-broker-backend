# DATA Act Core

The DATA Act Core is a collection of common components used by other DATA Act packages.

## Background

The U.S. Department of the Treasury is building a suite of open-source tools to help federal agencies comply with the [DATA Act](http://fedspendingtransparency.github.io/about/ "Federal Spending Transparency Background") and to deliver the resulting standardized federal spending information back to agencies and to the public.

For more information about the DATA Act Broker codebase, please visit this repository's [main README](../README.md "DATA Act Broker Backend README").

## Data Broker Core Database Reference

The DATA Act broker uses a single database (called `data_broker`) to track broker users, jobs and submissions, validation errors, and domain information needed to run data validations.

In addition, there is a `job_queue` database that holds a queue of jobs to be sent to the validator.

## DATA Act Core Project Layout

The DATA Act Core is a collection of common components used by other
DATA Act packages.  The directory structure is as follows:

```
dataactcore/
├── aws/            (Common AWS functions)
├── migrations/     (Alembic migration scripts)
├── credentials/    (Database credentials)
├── models/         (ORM models and database interfaces)
├── scripts/        (Utility and database setup scripts)
└── utils/          (JSON helper objects)
```

#### AWS

The `aws/` folder contains all of the common code that uses AWS Boto SDK, which requires the AWS CLI to function correctly. The installation instructions for the AWS CLI can be found in the [DATA Act installation guide](https://github.com/fedspendingtransparency/data-act-broker-backend/blob/master/doc/INSTALL.md).

#### Migrations

This contains the code for running migrations to different alembic versions of the database, and contains the version history for all alembic migrations.

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

Database interfaces are defined for logical data functions in the data broker (e.g., validations). Each interface inherits base functionality from `BaseInterface`, which defines the location of connection credentials and is responsible for overall session and connection handling. Where required, interfaces are extended to add additional functionality.

#### Scripts

The `scripts/` folder contains various python scripts used in the DATA Act broker backend install process as well as various utility scripts.

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
