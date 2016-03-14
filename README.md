# DATA Act Core

The DATA Act Core is a collection of common components used by other DATA Act repositories.

## Background

The U.S. Department of the Treasury is building a suite of open-source tools to help federal agencies comply with the [DATA Act](http://fedspendingtransparency.github.io/about/ "Federal Spending Transparency Background") and to deliver the resulting standardized federal spending information back to agencies and to the public.

One of these tools is the DATA Act Broker (broker). The broker ingests federal spending data from agency award and financial systems, validates it, and standardizes it against the [common DATA Act model](http://fedspendingtransparency.github.io/data-model/ "data model"). Treasury will make a hosted version of the broker freely available to agencies. Alternately, agencies can take this code and run the broker locally.

The broker comprises:

* The DATA Act core (you are here)
* The [broker's application programming interface (API)](https://github.com/fedspendingtransparency/data-act-broker "DATA Act broker API")
* The [DATA Act validator](https://github.com/fedspendingtransparency/data-act-validator "DATA Act validator")
* The [broker website](https://github.com/fedspendingtransparency/data-act-broker-web-app "DATA Act broker website")

## What Do I Need to Know?

If you're from a federal agency that will use Treasury's hosted DATA Act broker, you can probably stop reading here. Instead, refer to xxxxx [link to the user how-to for using the broker website].

If you're planning to use Treasury's hosted DATA Act broker but are interested in the under-the-hood details, refer to the *How It Works* documentation that lives with each DATA Act project. For this project (*i.e.*, the DATA Act core), you can find this information [below](https://github.com/fedspendingtransparency/data-act-core#how-it-works "How it Works - DATA Act core").

If you want to install and run the broker locally, read the [installation and setup](https://github.com/fedspendingtransparency/data-act-core#data-act-broker-installation-and-setup "DATA Act broker installation and setup") directions.

## DATA Act Broker Installation and Setup

If you want to install and run a local DATA Act broker at your agency, the instructions below are for you. Because we designed the broker's components to be used together, the installation and setup directions for the DATA Act core, broker API, validator, and broker website are consolidated here.

**Note:** If you're a developer who wants to contribute to this code or modify it for use at your agency, your setup instructions are a little different. See xxxxx [link to local dev install instructions].

### Install Back-end Prerequisites

The core, the broker API, and the validator compose the back-end of the DATA Act broker. The broker's website cannot work without the back-end, so we'll install these things first.

To run the back-end DATA Act broker applications, you'll need to have the following up and running on your machine:

* PostgreSQL
* Python

#### PostgreSQL

[PostgreSQL](https://en.wikipedia.org/wiki/PostgreSQL) is an object-relational database management system (ORDBMS) with an emphasis on extensibility and standards-compliance.

You can find PostgreSQL installers on [EnterpriseDB](http://www.enterprisedb.com/products-services-training/pgdownload). We recommend PostgreSQL 9.4.x. Download and run the correct installer for your operating system. As you proceed through the installation process, note your choices for port number and password. You will need those when setting up the broker. For other PostgreSQL installation options, more complete documentation is available on the PostgreSQL [wiki](https://wiki.postgresql.org/wiki/Detailed_installation_guides).

**Note:** If you're setting up the broker on Mac OSX, we recommend using [homebrew](http://brew.sh) to install PostgreSQL.

#### Python and Virtualenv

The broker's back-end components currently run on Python 2.7 (with the intention of migrating to Python 3x for the final release). Install Python 2.7 and pip (Python's package manager) using [these directions](https://docs.aws.amazon.com/cli/latest/userguide/installing.html#install-python "Install Python").

We highly recommend creating a [Python virtual environment](https://virtualenv.readthedocs.org/en/latest/installation.html "Python virtualenv") that will house the DATA Act broker components. A virtual environment (aka *virtualenv*) will isolate the broker software and its libraries from the libraries running on your local system and prevent potential conflicts with other software. In addition, we recommend using [virtualenvwrapper](https://virtualenvwrapper.readthedocs.org/en/latest/install.html "Python virtualenvwrapper documentation") to manage your Python environments.

1. Use pip to install virtualenv: `pip install virtualenv`
2. Use pip to install virtualenvwrapper: `pip install virtualenvwrapper` (*Windows users* should run `pip install virtualenvwrapper-win` instead).
3. TODO: instructions for setting WORKON_HOME, PROJECT_HOME and other dotfile junk you have to set up for virtualenvwrapper.
4. Create a virtual environment for the DATA Act software. In this example, we've named the environment `data-act`, but you can call it anything: `mkvirtualenv data-act`.
**Note:** If you're running multiple versions of Python on your machine, you can make sure your data act environment is running the correct Python version by specifying a Python version: `mkvirtualenv --python=[path to installed Python 2.7 executable] data-act`
5. You should see some `mkvirtualenv` output that looks similar to the example below. Essentially, this command creates a new directory named `data-act` with its own set of Python libraries. It also activates the `data-act` virtual environment. Anything you pip install from this point forward will be installed into the `data-act` environment rather than your machine's global Python environment. Your command line prompt indicates which (if any) virtualenv is active.

        rebeccasweger@GSA-xs-MacBook-Pro-4 ~
        $ mkvirtualenv --python=/usr/local/bin/python2.7 data-act-test
        Running virtualenv with interpreter /usr/local/bin/python2.7
        New python executable in data-act-test/bin/python2.7
        Also creating executable in data-act-test/bin/python
        Installing setuptools, pip...done.

        (data-act-test)
        rebeccasweger@GSA-xs-MacBook-Pro-4 ~


### Create and Configure Amazon Web Services (Optional)

You can configure the DATA Act broker to upload files to an Amazon Web Services (AWS) S3 bucket. This step is optional. If you're planning to run the broker without using and S3 bucket, skip the sections below and go right to [Installing Broker Backend Applications](https://docs.aws.amazon.com/cli/latest/userguide/installing.html#install-broker-backend-applications "Installing the broker backend apps").

**Are there additional instructions needed for people running w/o an S3 bucket?**

#### Creating S3 Only Role

The DATA Act Core repository allows for the creation of Security Token Service (STS) tokens that only allow for uploading files. To do this, an IAM Role needs to be created on the targeted AWS account. This role should have the following permission JSON, where the `s3-bucket-name` is the name of the S3 bucket:

```json
{
    "Version": "2016-01-29",
    "Statement": [
        {
            "Sid": "Stmt123456",
            "Effect": "Allow",
            "Action": [
                "s3:PutObjectAcl"
            ],
            "Resource": [
                "arn:aws:s3:::s3-bucket-name",
                "arn:aws:s3:::s3-bucket-name/*",
            ]
        }
    ]
}
```

In addition to the permission JSON, a Trust Relationship needs to be created for the target role, allowing the EC2 instance to assume the S3 uploading role during token creation.

#### Credentials

For the cloud environment, it is a best practice to use AWS roles for any EC2 instance running the Core. AWS roles provide a safe, automated key management mechanism to access and utilize AWS resources. At a minimum, the EC2 role should be granted Full S3 access permissions. Other repositories that use the core may have additional permissions required.

For local installations, credentials should be managed by the AWS CLI. This process is part of the install guide, which will walk users through the process.

#### Setup AWS Command Line Interface (CLI) Tools

Install the Python version of AWS Command Line Interface (CLI) tools following [these directions](https://docs.aws.amazon.com/cli/latest/userguide/installing.html#install-the-aws-cli-using-pip "install AWS CLI").

After you've installed the AWS CLI tools, configure them:

        $ aws configure

When prompted, enter:

* xxxxx for the `AWS Access Key ID`
* xxxxx for the `AWS Secret Access Key`
* xxxxx for the `Default regionn name [us-east-1]`
* xxxxx for the `Default output format [json]`

### Install Broker Backend Applications

Make sure that your Python virtual environment is activated (replace *data-act* with the name of the environment you created above):

        $ workon data-act

Use Python's pip installer to install each broker back-end application:

        $ pip install git+git://github.com/fedspendingtransparency/data-act-core.git@development
        $ pip install git+git://github.com/fedspendingtransparency/data-act-broker.git@dev
        $ pip install git+git://github.com/fedspendingtransparency/data-act-validator.git@development

### Initialize Broker Backend Applications

If everything installed correctly in the steps above, you're ready to initialize the broker back-end applications. To do this, you'll run a few scripts that set up the databases and create the configuration files. **Note:** The scripts below delete existing broker databases and data. DO NOT run them in a production environment.

Run the broker API initialization script:

        $ sudo webbroker -i
        [when prompted, ented your sudo password]

You will now answer a series of questions to guide you through broker configuration. If this is your first time through the install process, answer `y` when asked if `you would like to configure [x]`. Below is guidances for some of the questions, but the answers may vary depending on your specific environment:

* `Would you like to configure your S3 connection`: `y` if you're setting up an Amazon Web Service S3 bucket to use for broker file submission, `n` if you're planning to run everything locally with no cloud dependencies

    * `Enter you bucket name`: the name of the S3 your using for broker file submission
    * `Enter your S3 role`: the [AWS role you created](https://github.com/fedspendingtransparency/data-act-core#creating-s3-only-role "Creating S3-only role") for uploading files to the broker

* `Would you like to configure your logging`: `y` if you want to use the broker's cloud logging feature, `n` if you'd prefer to handle logging locally

    * `Enter port`: logging service port
    * `Enter the logging URL`: logging service URL

* `Would you like to configure your database connection`: `y` if this is your first time running the initialization script or if you need re-create the broker databases for a version change, `n` otherwise (saying yes will delete any exising broker data)

    * `database address`: `localhost` to connect to your local Postgres instance
    * `database port`: `5432` is the Postgres default. If you specified a different post during your [Postgres install](https://github.com/fedspendingtransparency/data-act-core#dPostgreSQL "PostgreSQL install"), use that instead.
    * `database username`: the username created during your [Postgres install](https://github.com/fedspendingtransparency/data-act-core#dPostgreSQL "PostgreSQL install")
    * `database password`: the password for the username created during your [Postgres install](https://github.com/fedspendingtransparency/data-act-core#dPostgreSQL "PostgreSQL install")
    * `Default database name`: `postgres`

* `Would you like to configure your broker web API:` `y`

    * `Broker API port`: the port you'll be running the broker on

* `Would you like to enable server side debugging`: `y`
* `Would you like to enable debug traces on REST requests`: `y`
* `Enter the allowed origin (website that will allow for CORS)`: `*`
* `url` for the DATA Act validator: the url where you will be running the validator service
* `Enter system e-mail address`: ???
* `Would you like to use a local dynamo database`: `n`
* `Enter the URL for the React application`: this is the website, yes?
* `Enter application security key` ????
* `Would you like to create the dynamo database table`: `n`
* `Would you like to configure the connection to the DATA Act validator`: `y`

        * `Enter url`: the full URL used for accessing the validator service (e.g., `http://locahost:3334`)

* `Would you like to configure the users to the DATA Act web api?`: `y`
* `Would you like to include test case users?`: 'y'

    * `Enter the admin user password:` password for the broker's test admin username
    * `Enter the admin user e-mail`: e-mail address for the broker's test admin username

When you're done, you should be a message saying that the broker has been initialized. If you want to change any of the options, you can run `sudo webroker -i` again.

Run the validator initialization script:

        $ sudo validator -i
        [when prompted, ented your sudo password]

Now you'll answer some questions about the validator configuration. YSome o Below is guidances for some of the questions, but the answers may vary depending on your specific environment:

* `Would you like to configure your database connection?`: `n` (you did this during the broker configuration)
* `Would you like to configure your validator web service?`: y
* `Enter web service port`: the port number you'd like to use when running the validator service
* `Would you like to enable server side debugging?`: `y`
* `Would you like to enable debug traces on REST requests?`: `y`
* `Would you like to configure your appropriations rules?`: `y`
* `Enter the full file path for your schema (appropriationsFields.csv)`: [site-packages folder of your virtual environment]/dataactvalidator/scripts/appropriationsFields.csv
* `Enter the full file path for your rules (appropriationsRules.csv)`: [site-packages folder of your virtual environment]/dataactvalidator/scripts/appropriationsRules.csv
* `Would you like to add a new TAS File?`: `y`
* `Enter the full file path for your TAS data (all_tas_betc.csv)`: [site-packages folder of your virtual environment]/dataactvalidator/scripts/all_tas_betc.csv

The validator will now initialize, which can take several minutes as the validator rules are loaded.

### Run Broker Backend Applications

After you've initialized the broker API and validator, start each of the services. For the broker API:

        $ webbroker -s

Once the broker API is started, you can check that it's running by visiting the URL specified when you initialized the application (that information is stored in the `dataactbroker` library:  `config/web_api_configuration.json` -- look for the `port` value). For example, if you're running the broker on localhost port 3333, visit `http://localhost:3333` in your browser. You should see the message `Broker is running`.

The process for starting the validator is similar:

        $ validator -s

If you don't remember the URL and port you specified for the validator during initialization, it's stored in the `dataactvalidator` library: `validator_configuration.json` (look for `host` and `port`). For example,if you're running the validator on localhost port 3334, visit `http://localhost:3334` in your browser. You should see the message `Validator is running`.


### Install Website Tools and Code

To the broker's website up and running, you'll need to install *Node.js* and a task runner called *Gulp.js*.

1. Download and run the Node.js installer for your operating system.
2. Use *npm*, Node's package manager to install the latest version of gulp (you'll need the latest version to ensure it can run the project's babel version of the `gulpfile`.):

        $ npm install gulp && npm install gulp -g

3. Get a copy of the website code to your local machine:

        `git clone https://github.com/fedspendingtransparency/data-act-broker-web-app.git`

4. There should now be a directory on your local machine called `data-act-broker-web-app`. Change to that directory:

        `cd data-act-broker-web-app`

### Start DATA Act Broker Website

By default, the broker's website will look for the broker API on localhost:80. If you want to override that, edit the `GlobalConstants_dev.js` file in the `data-act-broker-app` directory. Change the value for `API` to the URL where you're running the broker (for example, `http://localhost:3333/v1`).

Run the following commands to install the website's dependencies and run the site:

        $ npm install
        $ gulp

## DATA Act Core Details

The sections below provide some technical details about this repo and about some of the files and databases created during the broker initialization. If you already have the broker up and running (see the [install instructions]((https://github.com/fedspendingtransparency/data-act-core#data-act-broker-installation-and-setup "Install the DATA Act broker"))), you won't have to create any of the files or run any of the scripts below. The information is here for those interested in what's happening under-the-hood.

### Database Credentials

Information about this database is placed in a JSON file in your data-act-core installation: `dataactcore/credentials/dbCred.json`. It contains a JSON dictionary with keys `username`, `password`, `host`, and `port`. Below is an example of what should be in this file:

```json
{
    "username":"postgres",
    "password":"pass",
    "host":"localhost",
    "port":"5432"
}
```

### Setup Scripts

After creating the Postgres database and credentials file, several setup scripts should be run to create the databases and tables that will be used by the broker. In your data-act-core installation, there will be a folder [dataactcore/scripts/](https://github.com/fedspendingtransparency/data-act-core/tree/configuration/dataactcore/scripts). From within this folder, run the following commands:

```bash
$ python setupJobTrackerDB.py
$ python setupErrorDB.py
```

Finally, to prepare the validator to run checks against a specified set of fields and rules, your `data-act-validator` installation will have a [scripts/](https://github.com/fedspendingtransparency/data-act-validator/tree/configuration/dataactvalidator/scripts) folder containing scripts to create the rule sets for testing, as well as the following database setup scripts that must be run.

```bash
$ python setupStaging.py
$ python setupValidationDB.py
```

For example: `loadApprop.py` may be run to create the included rule set for testing an appropriations file, or you may replace `appropriationsFields.csv` and `appropriationsRules.csv` with custom versions to run a different set of rules.

If you want to use an updated list of Treasury Account Symbols for the validator checks, you'll need to get the updated [`all_tas_betc.csv`](https://www.sam.fms.treas.gov/SAMPublicApp/all_tas_betc.csv) file and place that in the [scripts/](https://github.com/fedspendingtransparency/data-act-validator/tree/configuration/dataactvalidator/scripts) folder before running:

```bash
$ python loadTas.py
$ python setupTASIndexs.py
```

Once these scripts have been run, the databases will contain everything they need to validate appropriations files.

### Data Broker Database Reference

After broker setup, there will be five databases:

* `error_data` - Holds file level errors in the `file_status` table, along with information about number of row level errors of each type in the `error_data` table. A complete list of every separate occurrence can be found in the error report csv file.
* `job_tracker` - Holds information on all validation and upload jobs, including status of jobs and relations between jobs. The `job_status` table is the main place to get this information and provides file name/type, status of the job, the job's associated submission, and the table in which the results are located. The `job_dependency` table details precedence constraints between jobs, giving the job IDs for both the prerequisite and the dependent job.
* `staging` - Holds records that passed validation. Each file validated will have a table in this database, named based on the job ID. If the `file_status` table in the `error_data` database lists the file as completed, each record in the input file will be present in either this staging table or the error report.
* `user_manager` - Holds a mapping between user names and user IDs to be used for providing submission history information to a user.
* `validation` - Contains all the information a submitted file is validated against. The `file_columns` table details what columns are expected in each file type, and the rule table maps all defined single-field rules to one of the columns specified in `file_columns`. The `multi_field_rule` table stores rules that involve a set of fields, but are still checked against a single record at a time. Finally, the `tas_lookup` table holds the set of valid TAS combinations, taken from the TAS csv file discussed in the setup section.

### How It Works

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

The `aws/` folder contains all of the common code that uses AWS Boto SDK, which requires the AWS CLI to function correctly. The installation instructions for the AWS CLI can be found in the [DATA Act installation guide](https://github.com/fedspendingtransparency/data-act-validator/tree/configuration/README.md#aws-cli-tools).

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

The `scripts/` folder contains various python scripts to setup parts of the DATA Act Core
repository for local installation. These scripts are used by the pip install process
to provide a seamless setup. See the [DATA Act installation guide](https://github.com/fedspendingtransparency/data-act-validator/tree/configuration/README.md#installation) for more details.
If needed, these scripts can be run manually to setup an environment.

`configure.py` provides interactive command line prompts to set the S3 bucket JSON and database access credentials. The S3 JSON format can be found in [AWS Setup](#aws-setup).  The databases credentials format can be found in the [Database Setup Guide](#database-setup-guide).

In addition to the JSON configuration scripts, database creation scripts are located in this folder. When run directly, the following scripts take no parameters and stand up all required tables within each database:

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
These provide a common way for other repositories to handle requests.

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
