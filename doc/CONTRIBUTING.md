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

### Install Xcode CLI (Mac users only)

Xcode Command Line Interface (CLI) is a prerequisite for installing several of the requirements on a Mac.

[This tutorial](https://developer.xamarin.com/guides/testcloud/calabash/configuring/osx/install-xcode-command-line-tools/ "Xcode CLI installation") covers setting up Xcode CLI on your Mac and checking if you have it installed.

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
   For Windows users, there may be extra steps needed.  If you run into an error on the import-module step, move the "VirtualEnvWrapper" folder from C:/Python27/Lib/site-packages/Users/*username*/Documents/WindowsPowerShell/Modules/ to C:/Users/*username*/Documents/WindowsPowerShell/Modules/.  Next, in powershell run the command "set-executionpolicy unrestricted".  Finally, in the VirtualEnvWrapper directory, open the file "VirtualEnvWrapperTabExpansion.psm1" and change "Function:TabExpansion" to "Function:TabExpansion2" in line 12. Windows users should also note that the virtualenvwrapper port can be problematic so if problems arise that you cannot get past, consider running without turning on virtualenvwrapper. This will cause the install changes to affect your entire system rather than just the virtualenv.
   
   **Note to Mac and Linux users:** After running:
   
   		pip install virtualenvwrapper
   		
   	your computer may not be able to find the commands to create a new virtualenv. Run:
   	
   		which virtualenvwrapper.sh
   		
   	to locate where pip installed virtualenvwrapper. For example, `/Library/Frameworks/Python.framework/Versions/3.5/bin/virtualenvwrapper.sh`. Add a line at the end of `~/.bash_profile` that points it to that location. You may also need to add a VIRTUALENVWRAPPER_PYTHON variable if it isn't recognizing the python version: 
   	
   		export VIRTUALENVWRAPPER_PYTHON=/usr/local/bin/python3
   		source /Library/Frameworks/Python.framework/Versions/3.5/bin/virtualenvwrapper.s
   		
   	After saving this change, run:
   		
   		$ source ~/.bash_profile
   	
   	This will point to the ~/.bash_profile to get setup details and paths and should allow you to run virtualenv properly.

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

Using AWS is optional, and by default the broker will not use these services. If you'd like to use AWS, [follow these directions](AWS.md "set up Amazon Web Services") now.

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

You will need to run two scripts to setup the broker's backend components. The first one creates the databases and loads the information needed to validate data submissions: schemas, rules, and domain values such as object classes and account codes. The second script creates a local admin user that you can use to log in. From the `data-act-broker-backend` directory:

        $ python dataactcore/scripts/initialize.py -i
        $ python dataactcore/scripts/initialize.py -a

**Important Notes:**
* By default, the broker installs with a small sample of [GTAS financial
  data](https://www.fiscal.treasury.gov/fsservices/gov/acctg/gtas/gtas_home.htm
  "GTAS"), which is used during the validation process. See the next section
  for more comprehensive options.

### Loading SF-133 data

If you'd like to install the broker using real GTAS data for your agency,
replace the sample file with data representing the GTAS periods you want to
validate against (using the same headers and data format as the sample file).
The files should be named `dataactvalidator/config/sf_133_yyyy_mm.csv`, where
`yyyy` is the fiscal year, and `mm` is the fiscal year period. This is only
necessary for local installs.

If instead, you want to match the production environment (and are a developer
on the DATA Act team), you can access our SF-133 files through S3. The data is
sensitive, so we do not host it publicly. In the `prod-data-act-submission`
bucket, within the `config` directory, you should see a series of
`sf_133_yyyy_mm.csv` files. Download these and store them in your local
`dataactvalidator/config` folder. 

Once you've placed those files, run:

```bash
python dataactvalidator/scripts/load_sf133.py
```

This will only load the new SF133 entries. To force load from your files, you
can add the `-f` or `--force` flag:

```bash
python dataactvalidator/scripts/load_sf133.py -f
```

This will take several minutes to process.

### Loading comparison data:

You may load your own comparison data by replacing the example file for each type of data within `dataactvalidator/config/` and running the initialization script with the appropriate flag. Note that the `use_aws` flag in the configuration must be set to `false` in order to load the data from a local file.


#### CGAC, Object Class, and Program Activity data

CGAC file location: `dataactvalidator/config/cgac.csv`

Object Class file location: `dataactvalidator/config/object_class.csv`

Program Activity file location: `dataactvalidator/config/program_activity.csv`

To load these files:
```bash
python dataactcore/scripts/initialize.py -d
```

#### TAS data:

TAS file location: `dataactvalidator/config/cars_tas.csv`

To load TAS data:
```bash
python dataactcore/scripts/initialize.py -t
```

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

## Debugging

### Logging Configuration

The default logging level for our loggers (and libraries we use) aren't always
verbose enough. Luckily, we can change the settings for any logger in our
local `dataactcore/config.yml`. For example, let's add print out all of the
sqlalchemy SQL calls:

```yaml
logging:
    python_config:
        loggers:
            sqlalchemy.engine:
                handlers: ['console']
                level: INFO
```

This modifies the `sqlalchemy.engine` logger (as well as
`sqlalchemy.engine.*`), changing the logging level.

See the
[docs](https://docs.python.org/3.4/library/logging.config.html#logging-config-dictschema)
for more configuration details. Everything within `python_config` is imported
via `dictConfig` (in addition to some standard settings defined in
`dataactcore.logging`.

### Adding log messages

Of course, if nothing is being logged, you won't be able to see application
state. To add log messages, you may need to create a logger at the top of the
module (i.e. *.py file). We should use `__name__` to name the loggers after
the modules they are used in.

```python
import logging


logger = logging.getLogger(__name__)
```

Then, use the logger by calling methods on it:

```python
logger.info('My message without parameters')
logger.warning('A bad thing happened to user %s', user_id)
try:
    raise ValueError()
except ValueError:
    logger.exception("Carries traceback info")
```

See the Python [docs](https://docs.python.org/3.4/library/logging.html) for
more info.
