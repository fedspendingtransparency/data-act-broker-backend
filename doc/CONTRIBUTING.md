## Public Domain

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the CC0 1.0 Universal public domain dedication.

All contributions to this project will be released under the CC0 dedication. By submitting a pull request, you are agreeing to comply with this waiver of copyright interest.

## Git Workflow

We use three main branches:

* `master` - stable code deployed to staging
* `development` - code in development that is periodically released to staging by merging into master
* `production` - Not currently in use since we're still in active development. Will be used to push code into production.

Only non-breaking, stable code should be merged into `master` and deployed to staging to prevent disruptions in other parts of the team. All code to be merged should be submitted via a pull request. Team members should _not_ merge their own pull requests but should instead request a code review first. The reviewing team member should merge the pull request after completing the review and ensuring it passes all continuous integration tests.

## Code Reviews

The Consumer Financial Protection Bureau has an excellent [code review guide](https://github.com/cfpb/front-end/blob/master/code-reviews.md).

## Continuous Integration

Our project uses Jenkins to run test suites and builds. Pull requests to master will automatically trigger jenkins to run the tests.

## Concluding a Sprint

At the conclusion of a sprint, all code for completed stories should be merged into `master` and deployed to staging.

The DATA Act broker contains several individual projects. The section below will walk you through the process of getting the entire code base up and running.

## DATA Act Broker Setup for Developers

If you're a developer, you may want want to set the broker in your preferred development environment rather than using the Docker container.

Assumptions:

* You're able to install software on your local machine
* You have git installed on your machine and are able to clone code repositories from GitHub. If this isn't the case, the easiest way to get started is to install [GitHub Desktop](https://desktop.github.com/ "GitHub desktop"), available for Windows or Mac.
* You're familiar with opening a terminal on your machine and using the command line as needed.

### Install PostgreSQL

[PostgreSQL](https://en.wikipedia.org/wiki/PostgreSQL) is an object-relational database management system (ORDBMS) with an emphasis on extensibility and standards-compliance.

1. Download the correct PostgreSQL installer for your operating system from [EnterpriseDB](http://www.enterprisedb.com/products-services-training/pgdownload) (we recommend PostgreSQL 9.4.x).
2. Run the installer. As you proceed through the installation wizard, note your choices for port number, username, and password. You will need those when creating the broker's configuration file.

More complete install documentation is available on the PostgreSQL [wiki](https://wiki.postgresql.org/wiki/Detailed_installation_guides).

**Note:** If you're setting up the broker on Mac OSX, we recommend using [homebrew](http://brew.sh) to install PostgreSQL.

### Install Python and Create Virtual Environment

The broker's backend components currently run on Python 2.7 (with the intention of migrating to Python 3x for the final release). These instructions will walk you through the process of installing Python and creating a Python-based virtual environment to house the DATA Act backend components. A virtual environment will isolate the broker software and its libraries from those running on your local system and prevent potential conflicts.

If you already have a Python development environment on your machine and a preferred way of managing it, feel free to skip to the next section. We wrote the directions below for folks who don't have a Python environment up and running yet and need the quickest way to get started.

1. Download the miniconda installer: [http://conda.pydata.org/miniconda.html](http://conda.pydata.org/miniconda.html "Miniconda installers"). *Windows Users:* Choose the [Python 2.7 32-bit installer](https://repo.continuum.io/miniconda/Miniconda-latest-Windows-x86.exe "Python 2.7 Windows 32-bit installer"), even if you're using a 64-bit Windows machine.
2. Run the miniconda installer. When prompted for _Advanced Installation Options_, accept the defaults.
3. After miniconda installs, open a terminal window and type the following command to create your DATA Act virtual environment. These instructions assume that _data-act_ will be the name of your environment, but you can name it anything.

        $ conda create --name data-act python=2.7.9

4. You should see an informational message about some packages that miniconda will download and install. At the prompt, type _y_ to proceed.
5. Once miniconda is finished creating your environment, activate it:

        $ activate data-act

This new environment will be active until you run the `deactivate` command. You can re-activate the environment again at any time by typing `activate data-act`.

### Configure Amazon Web Services (Optional)

When running the broker, you have the option to use Amazon Web Services (AWS) to handle:

* Storage of data submissions and validation reports (via S3 buckets).
* Session management (via DynamoDB).

Using AWS is optional, and by default the broker will not use these services. If you'd like to use AWS, [follow these directions](AWS.md "set up Amazon Web Services") now.

### Install Local DynamoDB (non-AWS users only)

If you're not using AWS tools when running the broker, you'll need to install a local version of Amazon's DynamoDB to handle session management. The local DynamoDB is a Java executable the runs on Windows, Mac, and Linux systems.

1. Install version 7+ of the [Java Development Kit (jdk)](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html "download Java Development Kit").
2. Download the [local DynamoDB files](http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.zip "download local DynamoDB").
3. Follow [Amazon's instructions](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html "running DynamoDB on your computer") to run local DynamoDB using the downloaded files.

Don't worry about creating tables in DynamoDB: the broker's initialization process handles that.

**Note:** The local version of DynamoDB is not recommend for production.

### Clone Broker Code Repositories

Now we're ready to install the DATA Act broker itself. Before starting:

* These directions involve the command line. If you're on Windows, use the Git shell that comes with the Windows git install or the GitHub desktop install. Don't use the Windows command prompt.
* Decide where on your machine you want the DATA Act code to live. Throughout these directions, we'll refer to this directory as your `project home` and use it as your starting point.
* Make sure that your DATA Act Python environment is activated:

        $ activate data-act

#### DATA Act Core

Navigate to your DATA Act project home. From the command line, clone the DATA Act Core repository from GitHub to your local environment:

        $ git clone git@github.com:fedspendingtransparency/data-act-core.git

Navigate to the DATA Act core's main folder:

        $ cd data-act-core

Install the code and its dependencies:

        $ pip install -e .

#### Broker API

Navigate back to your DATA Act project home. From the command line, clone the DATA Act Broker API repository from GitHub to your local environment:

        $ git clone git@github.com:fedspendingtransparency/data-act-broker.git

Navigate to the broker API's main folder:

        $ cd data-act-broker

Install the code and its dependencies:

        $ pip install -e .

#### Validator

Navigate back to your DATA Act project home. From the command line, clone the DATA Act Validator repository from GitHub to your local environment:

        $ git clone git@github.com:fedspendingtransparency/data-act-validator.git

Navigate to the validator's main folder:

        $ cd data-act-validator

Install the code and its dependencies:

        $ pip install -e .

#### Broker Website

Navigate back to your project home. From the command line, clone the DATA Act web app repository from GitHub to your local environment:

        $ git clone git@github.com:fedspendingtransparency/data-act-broker-web-app.git

### Create Broker Config File

Before running the broker, you'll need to provide a few configuration options. Use the provided sample config file as a starting point:

1. Navigate to the location of the data-act-core code.
2. From the `data-act-core` directory, go to `dataactcore` and open the file called `config_example.yml` in a text editor.
3. Save `config_example.yml` as `config.yml`.
4. Update the values in `config.yml` as appropriate for your environment. In many cases the default values will work just fine. The most important config values to change when getting started are:

    * under _broker_:
        * `full_url`
        * `reply_to_email`
        * `admin_email`
        * `admin_password`
        * `broker_files`
    * under _services_:
        * `error_report_path`
    * under _db_:
        * `username`
        * `password`
    * under _logging_:
        * `log_files`
5. Save your changes to config.yml

### Initialize Broker Backend Applications

You will need to run two scripts to setup the broker's backend components. These create the necessary databases and data.

        $ webbroker -i
        $ validator -i

### Run Broker Backend Applications

After you've initialized the broker API and validator, start the broker API:

From the `data-act-broker` directory:

        $ python dataactbroker/app.py

Make sure the broker API is working by visiting the hostname and port specified in the config file (`broker_api_host` and `broker_api_port`). For example, if you're running the broker on localhost port 3333, visit `http://localhost:3333` in your browser. You should see the message `Broker is running`.

The process for starting the validator is similar. From the `data-act-validator` directory:

        $ python dataactvalidator/app.py

Make sure the validator is working by visiting by visiting the hostname and port specified in the config file (`validator_host` and `validator_port`) For example, if you're running the validator on localhost port 3334, visit `http://localhost:3334` in your browser. You should see the message `Validator is running`.

### Setup and Run Broker Website

Once the DATA Act broker's back-end is up and running, setup and start the corresponding website:

1. Download and run the Node.js installer for your operating system: [https://nodejs.org/en/](https://nodejs.org/en/ "Node.js installer").
2. Use *npm* (Node's package manager, which is part of the Node.js install) to install the latest version of gulp:

        $ npm install gulp && npm install gulp -g

3. Change to the `data-act-broker-web-app` directory that was created when you cloned the DATA Act web repository.
4. Open the `GlobalConstants_dev.js` file in the `data-act-broker-app` directory. Change the value for `API` to the URL where you're running the broker (for example, `http://localhost:3333/v1`).
5. Run the following commands to install the website's dependencies and run the site:

        $ npm install
        $ gulp

Your browser will open to the broker website's login page. Log in with the admin e-mail and password you set in the broker config file (`admin_email` and `admin_password`).


## Database Migrations

If part of your DATA Act broker development involves changing the database models,
use the following process for generating database migration files. We're using Alembic to create and run database migrations, which is installed as part of the broker.

### Running Migrations

Before doing your first migration, drop all tables and run
```bash
$ alembic upgrade head
```
This will create the alembic_version table needed for the migration process

After making updates to the models, run the following in ```dataactcore/``` to autogenerate the migration script:
```bash
$ alembic revision --autogenerate -m [file name]
```
[file name] should correspond to the changes made to the models, e.g., "create users table" or "add email column to users table". You will now see that a new file called ```[revision #]_[file name].py``` in ```dataactcore/migrations/versions/``` which contains the generated code for the database schema changes.

Verify that the new revision file is making the intended alterations. Then run the following command in order to implement all new revisions:
```bash
$ alembic upgrade head
```

This will consequently update the table ```alembic_version``` with the latest revision number.

In order to revert to a specific revision run the following, where [revision] corresponds to the revision to revert to:
```bash
$ alembic downgrade [revision]
```
