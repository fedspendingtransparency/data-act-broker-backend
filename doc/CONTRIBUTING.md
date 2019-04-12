Contributing Code to DATA Act Broker
====
_Follow the development process and checks below in order to promote candidate code changes to production._

### Git Workflow

We use three main branches:

* `staging` - Stable code deployed to a staging version of the data broker
* `development` - Code in development that is released to `staging` at the close of each sprint
* `master` - Code on the production site. Code gets merged to this branch by the product owner once it has been tested on staging.

Only non-breaking, stable code is merged into `development` and promoted to higher branches in order to prevent disruptions to users and team members.

All code to be merged should be submitted to the `development` branch via a GitHub pull request. The pull request template is available [here](/pull_request_template.md "Pull Request Template"), and faciliates code reviews and quality checks.

### Continuous Integration

Pull requests must pass all GitHub checks on the PR, including Travis CI tests. See the [Travis configuration file](/.travis.yml "Travis Configuration").

To run tests locally, see documentation for each app:

* [Broker API tests](../dataactbroker/README.md#automated-tests)
* [Broker Validator tests](../dataactvalidator/README.md#automated-tests)

### Concluding a Sprint

At the conclusion of a sprint, new code merged into the `development` as part of approved PRs is merged into `staging`. It is then tested and merged in to `master` when ready, as part of its release to production.

The DATA Act Broker contains several individual components. The section below walks through the process of getting the entire code base up and running.

## Local Development Environment Setup
_To run Broker locally, and test code changes, you must setup a local development environment_

Start by following instructions in [INSTALL.md](INSTALL.md "broker install guide") to get all the broker components up and running as Docker containers in a local development environment. 

_**Contributing to the Broker Website**_

This setup should provide a running Broker frontend web application as a container, which you can browse to. If you want to contribute changes to the frontend application, see instructions for developing Broker Frontend in the [Broker web app code repository](https://github.com/fedspendingtransparency/data-act-broker-web-app "DATA Act broker web app").

_**Pointing Containerized Broker at an Existing Postgres Database**_

NOTE: If you would rather have your `dataact-broker-backend` and `dataact-broker-validator` containers to instead connect to PostgreSQL running on your host machine, you must change your config to route from within docker containers to your host IP. 
* Change `db.host` config param in `local_config.yml` to `host.docker.internal` (or `docker.for.mac.host.internal` if using docker v `17.12.0`-`18.03.0`).
* Change the `db.username` and `db.password` config params for your database in `local_secrets.yml`

### Python Development
_Setup python on your host machine to work with the source code and its dependent libraries_

#### Requirements
Ensure the following dependencies are installed and working prior to continuing:

- [`python3`](https://docs.python-guide.org/starting/installation/#python-3-installation-guides)
- [`pyenv`](https://github.com/pyenv/pyenv/#installation) using Python 3.5.x
  - _NOTE: Read full install. `brew install` needs to be followed by additional steps to modify and source your `~/.bash_profile`_
- `Bash` or another Unix Shell equivalent
  - Bash is available on Windows as [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10)
- Command line package manager
  - Windows' WSL bash uses `apt-get`
  - OSX users will use [`Homebrew`](https://brew.sh/)

#### Setup
Navigate to the base directory for the Broker backend source code repositories
```bash
$ cd data-act-broker-backend
```

Create and activate the virtual environment using `venv`, and ensure the right version of Python 3.5.x is being used (the latest RHEL package available for `python35u`, currently 3.5.5)

```bash
$ pyenv install 3.5.5
$ pyenv local 3.5.5
$ python -m venv .venv/broker-backend
$ source .venv/broker-backend/bin/activate
```

Your prompt should then look as below to show you are _in_ the virtual environment named `broker-backend` (_to exit that virtual environment, simply type `deactivate` at the prompt_).

```bash
(broker-backend) $
``` 

[`pip`](https://pip.pypa.io/en/stable/installing/) `install` application dependencies

:bulb: _(try a different WiFi if your current one blocks dependency downloads)_
```bash
(broker-backend) $ pip install -r requirements.txt
```

### Database Migrations

If part of your DATA Act broker development involves changing the database models, use the following process for generating database migration files. We're using Alembic to create and run database migrations, which is installed as part of the broker.

#### Running Migrations

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

### Configure Amazon Web Services (Optional)

When running the broker, you have the option to use Amazon Web Services (AWS) to handle:

* Storage of data submissions and validation reports (via S3 buckets).

Using AWS is optional, and by default the broker will not use these services. If you'd like to use AWS, [follow these directions](AWS.md "set up Amazon Web Services") now.

### Debugging

#### Logging Configuration

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

#### Adding log messages

Of course, if nothing is being logged, you won't be able to see application state. To add log messages, you may need to create a logger at the top of the module (i.e. *.py file). We should use `__name__` to name the loggers after the modules they are used in.

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

## Public Domain License

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the CC0 1.0 Universal public domain dedication.

All contributions to this project will be released under the CC0 dedication. By submitting a pull request, you are agreeing to comply with this waiver of copyright interest.