## Public Domain

This project is in the public domain within the United States, and copyright and related rights in the work worldwide are waived through the CC0 1.0 Universal public domain dedication.

All contributions to this project will be released under the CC0 dedication. By submitting a pull request, you are agreeing to comply with this waiver of copyright interest.

## Git Workflow

All code to be merged should be submitted to `development` via a Github pull request. The pull request template is available [here](/pull_request_template.md "Pull Request Template").

## Continuous Integration

Pull requests must pass Travis CI tests. See [the Travis configuration file](/.travis.yml "Travis Configuration").

## Load or Update Domain Data

You will need to run two scripts to setup the broker's backend components. From the `data-act-broker-backend` directory:

```bash
python dataactcore/scripts/initialize.py -i
python dataactcore/scripts/initialize.py -a
```

The first one creates the databases and loads the information needed to validate data submissions: schemas, rules, and domain values such as object classes and account codes. You can view what each function does [here](https://github.com/fedspendingtransparency/data-act-broker-backend/blob/master/dataactcore/scripts/initialize.py), but the functions called by `initialize.py -i` are as follows:

```python
setup_db()
load_sql_rules()
load_domain_value_files(validator_config_path)
load_agency_data(validator_config_path)
load_tas_lookup()
load_sf133()
load_validator_schema()
load_location_codes()
load_zip_codes()
load_offices()
```

The second script creates a local admin user that you can use to log in. The Broker utilizes MAX.gov for login when using a remote server, but we cannot recieve their response locally so we use a username and password for local development login. The administrative user created will utilize the credentials defined in your `config.yml` file.

**Important Notes:**
* By default, the broker installs with a small sample of [GTAS financial data](https://www.fiscal.treasury.gov/fsservices/gov/acctg/gtas/gtas_home.htm "GTAS"), which is used during the validation process. See the next section for more comprehensive options.

#### Loading SF-133 data

If you'd like to install the broker using real GTAS data for your agency, replace the sample file with data representing the GTAS periods you want to validate against (using the same headers and data format as the sample file). The files should be named `dataactvalidator/config/sf_133_yyyy_mm.csv`, where `yyyy` is the fiscal year, and `mm` is the fiscal year period. This is only necessary for local installs.

If instead, you want to match the production environment (and are a developer on the DATA Act team), you can access our SF-133 files through S3. The data is sensitive, so we do not host it publicly. In the `prod-data-act-submission` bucket, within the `config` directory, you should see a series of `sf_133_yyyy_mm.csv` files. Download these and store them in your local `dataactvalidator/config` folder.

Once you've placed those files, run:

```bash
python dataactvalidator/scripts/load_sf133.py
```

This will only load the new SF133 entries. To force load from your files, you can add the `-f` or `--force` flag:

```bash
python dataactvalidator/scripts/load_sf133.py -f
```

This will take several minutes to process.

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

### Setup and Run Broker Website

Once the DATA Act broker's backend is up and running, you may also want to stand up a local version of the broker website. The directions for doing that are in the [website project's code repository](https://github.com/fedspendingtransparency/data-act-broker-web-app "DATA Act broker website").

After following the website setup directions, you can log in with the admin e-mail and password you set in the [broker's backend config file](#create-broker-config-file "config file setup") (`admin_email` and `admin_password`).

## Database Migrations

If part of your DATA Act broker development involves changing the database models, use the following process for generating database migration files. We're using Alembic to create and run database migrations, which is installed as part of the broker.

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
