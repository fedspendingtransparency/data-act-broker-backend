## Public Domain

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the CC0 1.0 Universal public domain dedication.

All contributions to this project will be released under the CC0 dedication. By submitting a pull request, you are agreeing to comply with this waiver of copyright interest.

## Git Workflow

We use three main branches:

* `staging` - Stable code deployed to a staging version of the data broker
* `development` - Code in development that is released to `staging` at the close of each sprint
* `master` - Code on the production site. Code gets merged to this branch by the product owner once it has been tested on staging.

Only non-breaking, stable code should be merged into `development`, `staging`, and `master` to prevent disruptions to users and team members.

All code to be merged should be submitted to `development` via a pull request. Team members should _not_ merge their own pull requests but should instead request a code review first. The reviewing team member should merge the pull request after completing the review and ensuring it passes all continuous integration tests.

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
2. Run the installer. As you proceed through the installation wizard, note your choices for port number, username, and password. You will need those when creating the broker's configuration files.

More complete install documentation is available on the PostgreSQL [wiki](https://wiki.postgresql.org/wiki/Detailed_installation_guides).

**Note:** If you're setting up the broker on Mac OSX, we recommend using [homebrew](http://brew.sh) to install PostgreSQL.

**Note:** The database user will need to have permission to create databases.
A "superuser" will do.

### Install Python and Create Virtual Environment

The broker's backend components currently run on Python 3.x. These instructions will walk you through the process of installing Python and creating a Python-based virtual environment to house the DATA Act backend components. A virtual environment will isolate the broker software and its libraries from those running on your local system and prevent potential conflicts.

If you already have a Python development environment on your machine and a preferred way of managing it, feel free to skip to the next section. We wrote the directions below for folks who don't have a Python environment up and running yet and need the quickest way to get started.

1. Install Python 3.x:
    * Windows and OSX users can download a 3.x Python installer here: [https://www.python.org/downloads/](https://www.python.org/downloads/ "Python installer downloads")
    * Linux users can install Python 3.x using their distribution's package manager.

2. Use pip to install virtualenv (pip is Python's package manager and is automatically installed with Python 3.x):

        pip install virtualenv
3. Use pip to install virtualenvwrapper:

        pip install virtualenvwrapper

4. Tell virtualenvwrapper where on your machine to create virtual environments and add it to your profile. This is a one-time virtualenvwrapper setup step, and the process varies by operating system. [This tutorial](http://newcoder.io/begin/setup-your-machine/ "Python: setting up your computer") covers setting up virtualenvwrapper on OSX, Linux, and Windows.
   For Windows users, there may be extra steps needed.  If you run into an error on the import-module step, move the "VirtualEnvWrapper" folder from C:/Python27/Lib/site-packages/Users/*username*/Documents/WindowsPowerShell/Modules/ to C:/Users/*username*/Documents/WindowsPowerShell/Modules/.  Next, in powershell run the command "set-executionpolicy unrestricted".  Finally, in the VirtualEnvWrapper directory, open the file "VirtualEnvWrapperTabExpansion.psm1" and change "Function:TabExpansion" to "Function:TabExpansion2" in line 12.

5. Create a virtual environment for the DATA Act software. In this example we've named the environment *data-act*, but you can call it anything:

        mkvirtualenv data-act

    **Note:** If you're running multiple versions of Python on your machine, you can make sure your data act environment is running the correct Python version by pointing to a specific binary

        mkvirtualenv --python=[path to installed Python 3.x executable] data-act

6. You should see some output that looks similar to the example below. Essentially, this command creates and activates a new virtualenv named `data-act` with its own set of Python libraries.  Anything you pip install from this point forward will be installed into the *data-act* environment rather than your machine's global Python environment. Your command line prompt indicates which (if any) virtualenv is active.

 **Note:** in the command below, replace `/usr/local/bin/python3.4` with the path to your local Python 3.x executable.

        $ mkvirtualenv --python=/usr/local/bin/python3.4 data-act
        Running virtualenv with interpreter /usr/local/bin/python3.4
        New python executable in data-act-test/bin/python3.4
        Also creating executable in data-act-test/bin/python
        Installing setuptools, pip...done.

        (data-act)

7. This new environment will be active until you run the `deactivate` command. You can re-activate the environment again at any time by typing `workon data-act`.

### Configure Amazon Web Services (Optional)

When running the broker, you have the option to use Amazon Web Services (AWS) to handle:

* Storage of data submissions and validation reports (via S3 buckets).
* Session management (via DynamoDB).

Using AWS is optional, and by default the broker will not use these services. If you'd like to use AWS, [follow these directions](AWS.md "set up Amazon Web Services") now.

### Install Local DynamoDB (non-AWS users only)

If you're not using AWS tools when running the broker, you'll need to install a local version of Amazon's DynamoDB to handle session management. The local DynamoDB is a Java executable the runs on Windows, Mac, and Linux systems.

1. Install version 7+ of the [Java Development Kit (jdk)](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html "download Java Development Kit").
2. Follow [Amazon's instructions](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html#Tools.DynamoDBLocal.DownloadingAndRunning "running DynamoDB on your computer") for downloading and running local DynamoDB. You should be able to start the local DynamoDB using the example command provided by Amazon, without overriding any of the default options.

Don't worry about setting DynamoDB endpoints or creating tables: the broker's code handles this.

**Note:** The local version of DynamoDB is not recommended for production.

### Install RabbitMQ

RabbitMQ is used to pass jobs to the validator, and requires Erlang to be installed before RabbitMQ.

1.  Install Erlang based on the [download instructions](https://www.erlang.org/downloads)
2.  Choose an installation guide based on your OS for [RabbitMQ](https://www.rabbitmq.com/download.html).  Be sure to install Erlang before installing RabbitMQ.  The default user and password is "guest"/"guest", if you change these you'll need to keep that information to be placed in the config files later in the process.

### Clone Broker Backend Code Repository

Now we're ready to install the DATA Act broker itself. Before starting:

* These directions involve the command line. If you're on Windows, use the Git shell that comes with the Windows git install or the GitHub desktop install. Don't use the Windows command prompt.
* Make sure that your DATA Act Python environment is activated:

        $ workon data-act

#### DATA Act Broker Backend

Decide where on your machine you want the DATA Act broker code to live. From the command line, navigate there and clone the DATA Act Broker Backend repository from GitHub to your local environment:

        $ git clone https://github.com/fedspendingtransparency/data-act-broker-backend.git

Navigate to the DATA Act Broker Backend's main folder:

        $ cd data-act-broker-backend

Switch to the alpha release version of the code. This is the latest stable release.

        $ git checkout v0.1.0-alpha

**Note:** If you'd rather use the latest, work-in-progress features of the DATA Act broker, replace the above command with `git checkout staging`.

Install the dependencies.  If you are installing on Windows, you may get an error on the "uwsgi" package.  This is not necessary for local installs, so this error can be ignored, and you can proceed to the next section after getting this error.

        $ pip install -r requirements.txt

### Update $PYTHONPATH

The backend components import Python modules from one another. Therefore, the locations of these modules need to be on your $PYTHONPATH. Use the virtualenvwrapper [add2virtual](http://virtualenvwrapper.readthedocs.org/en/latest/command_ref.html#path-management "virtualenvwrapper path management") shortcut to add them:
If you are using virtualenvwrapper-powershell in Windows, the following command may not work.  In that case, you can add this directory to your PATH or PYTHONPATH environment variable.

        $ add2virtualenv [location of your code]/data-act-broker-backend

### Create Broker Config Files

Before running the broker, you'll need to provide a few configuration options. The broker uses three config files:

* `config.yml`: Config parameters shared across broker instances (_e.g._, production, staging, development, local).
* `[instance]_config.yml`: Config parameters specific to a broker instance (_e.g._, location of the development database, url for the development broker instance). Values in this file override their corresponding values in `config.yml`.
* `[instance]_secrets.yml`: Sensitive config values specific to a broker instance (_e.g._, database passwords).

At startup, the broker looks for an environment variable called `env` and will use that to set the instance name. If there is no `env` environment variable, the broker will set the instance name to `local` and look for `local_config.yml` and `local_secrets.yml`.

There are sample config files in `data-act-broker-backend/dataactcore`. Use these as a starting point when setting up the broker. The instructions below assume that you're installing the broker for local development.

1. Open `config_example.yml` in a text editor and save it as `config.yml`.
2. Update the values in `config.yml` as appropriate for your installation and save. In many cases the default values will work just fine. The most important config values to change will be in `local_config.yml` and `local_secrets.yml`
3. Open `local_config_example.yml` in a text editor and save it as `local_config.yml`.
4. Update the values in `local_config.yml` as appropriate for your installation and save.
5. Open `local_secrets_example.yml` in a text editor and save it as `local_secrets.yml`.
6. Update the values in `local_secrets.yml` as appropriate for your installation and save.

### Initialize Broker Backend Applications

You will need to run two scripts to setup the broker's backend components. These create the necessary databases and data. From the `data-act-broker-backend` directory:

        $ python dataactbroker/scripts/initialize.py -i
        $ python dataactvalidator/scripts/initialize.py -i

**Important Notes:**
* If you're using a local DynamoDB, make sure it's running before executing these scripts.
* By default, the broker installs with a small sample of [GTAS financial data](https://www.fiscal.treasury.gov/fsservices/gov/acctg/gtas/gtas_home.htm "GTAS"), which is used during the validation process. If you'd like to install the broker using real GTAS data for your agency, replace the sample file with data representing the GTAS periods you want to validate against (using the same headers and data format as the sample file). The files should be named `dataactvalidator/config/sf_133_yyyy_mm.csv`, where `yyyy` is the fiscal year, and `mm` is the fiscal year period. This is only necessary for local installs.

### Run Broker Backend Applications

After you've initialized the broker API and validator, start the broker API:

From the `data-act-broker-backend` directory:

        $ python dataactbroker/app.py

Make sure the broker API is working by visiting the hostname and port specified in the config file (`broker_api_host` and `broker_api_port`). For example, if you're running the broker on localhost port 3333, visit `http://localhost:3333` in your browser. You should see the message `Broker is running`.

The process for starting the validator is similar. From the `data-act-broker-backend` directory:

        $ python dataactvalidator/app.py

Make sure the validator is working by visiting by visiting the hostname and port specified in the config file (`validator_host` and `validator_port`) For example, if you're running the validator on localhost port 3334, visit `http://localhost:3334` in your browser. You should see the message `Validator is running`.

### Setup and Run Broker Website

Once the DATA Act broker's backend is up and running, you may also want to stand up a local version of the broker website. The directions for doing that are in the [website project's code repository](https://github.com/fedspendingtransparency/data-act-broker-web-app "DATA Act broker website").

After following the website setup directions, you can log in with the admin e-mail and password you set in the [broker's backend config file](#create-broker-config-file "config file setup") (`admin_email` and `admin_password`).


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
