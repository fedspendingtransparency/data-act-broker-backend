DATA Act Broker Installation and Setup
====

The easiest way to run a local DATA Act Broker is to use our Docker images. Essentially, the images are a packaged version of the broker backend components that is pre-configured and will run in an isolated environment.

If you're a developer who wants to run the broker on your machine and make changes to the code, please see the project's contributing guide: [CONTRIBUTING.md](CONTRIBUTING.md "project contributing guide").

At a glance, setup steps are:

1. Acquire the source code
2. Edit a set of configuration files
3. Run the containers using Docker
4. Seed the database with data

At this point you will have a functioning backend API component and validator component. To interact with the broker through a web browser, install the frontend application

**Assumptions**

* You are able to install software on your local machine
* You have `git` installed on your machine and are able to clone code repositories from GitHub. If this isn't the case, the easiest way to get started is to install [GitHub Desktop](https://desktop.github.com/ "GitHub desktop"), available for Windows or Mac.
* You have `docker` installed and running on your machine, with`docker-compose`. If not, get [Docker Desktop](https://www.docker.com/products/docker-desktop.)
* You are familiar with opening a terminal on your machine and using the command line as needed.

## Code
Create a base directory for the Broker code repositories

```
$ mkdir -p broker && cd broker
$ git clone https://github.com/fedspendingtransparency/data-act-broker-backend.git
$ git clone https://github.com/fedspendingtransparency/data-act-broker-web-app.git
$ cd data-act-broker-backend
```

## Config
Before running the broker, you'll need to provide a few configuration options. Each module consuming configuration data in the broker gets values for that configuration data from three config files within the `dataactcore/` folder:

* `config.yml`: Default config parameters common to instances of all broker environment types (_e.g._, production, staging, development, local).
* `<instance>_config.yml`: Environment instance-specific config parameters specific to a particular broker environment instance (_e.g._, location of the development database, url for the development broker instance). Values in this file override their corresponding values in `config.yml`.
* `<instance>_secrets.yml`: Sensitive config values specific to a broker environment (_e.g._, database passwords).

At startup, the broker looks for an environment variable called `env` and will use that to set the environment instance name. If there is no `env` environment variable, the broker will set the instance name to `local` and look for `local_config.yml` and `local_secrets.yml`.

For example, when `env` is set to or defaults to `local`, these are the list of files needed to complete setup.

```
        dataactcore/config.yml
        dataactcore/local_config.yml
        dataactcore/local_secrets.yml

        dataactvalidator/config/agency_codes_list.csv
        dataactvalidator/config/cars_tas.csv
        dataactvalidator/config/cgac.csv
        dataactvalidator/config/object_class.csv
        dataactvalidator/config/program_activity.csv
```

To get up and running quickly, you can use _example_ config and data files that are provided, for a local instance. To do so, run the copy commands below _(run from `data-act-broker-backend` folder)_:

_**Copy Config Files**_

```
$ cp dataactcore/config_example.yml dataactcore/config.yml
$ cp dataactcore/local_config_example.yml dataactcore/local_config.yml
$ cp dataactcore/local_secrets_example.yml dataactcore/local_secrets.yml
```
_**Copy Data Files**_
```
$ cp dataactvalidator/config/example_agency_codes_list.csv dataactvalidator/config/agency_codes_list.csv
$ cp dataactvalidator/config/example_cars_tas.csv dataactvalidator/config/cars_tas.csv
$ cp dataactvalidator/config/example_cgac.csv dataactvalidator/config/cgac.csv
$ cp dataactvalidator/config/example_object_class.csv dataactvalidator/config/object_class.csv
$ cp dataactvalidator/config/example_program_activity.csv dataactvalidator/config/program_activity.csv
```

## Run with Docker

First, check that Docker and Docker Compose are running 
```
$ docker version
$ docker-compose version
```
You should get valid responses from each command, reporting the version.

Next, ensure you have the latest build of the DATA Act Broker images, based on your updated source code. Run the following to instruct docker to rebuild the images used by Broker from the source code.
_NOTE: This uses **two** source code repositories that you downloaded at the beginning: `data-act-broker-backend` and `data-act-broker-web-app`_

```
$ docker-compose build
```

Now start the containers using these built images. 
```
$ docker-compose up
```
_This command will: _

- _spin up the PostgreSQL database as a container named `dataact-broker-db`_
- _spin up the Broker backend API component as a container named `dataact-broker-backend`_
- _spin up the Broker Validator component as a containeir named `dataact-broker-validator`_
- _spin up the Broker frontend JavaScript application as a container named `dataact-broker-frontend`_

It may take about 30 seconds for all containers to report as running and ready.

Test connectivity:

* To the API:
```
$ curl http://localhost:9999/v1/current_user/
```
```
{"message": "Login Required"}
```
_(This is fine, we haven't loaded data yet)_

* To the frontend web app: brows to http://localhost:3000

Take note of these commands are useful when working with Docker Compose:

- `docker-compose up --detach` - runs your containers in the background, without writing out the logs to the console
- `docker-compose logs` - view the latest logs from running containers
- `docker logs <container-name>` - view the latest logs for just one container
- `docker-compose logs -tf` or `docker logs <container-name> -tf` - tail live logs of container(s)
- `docker-compose up --deteach --no-deps --force-recreate <container-name>` recreate and restart a single container (for example, if source code for it changed)
- `docker-compose down` shuts down all your local containers and removes them. This may help debug some problems. You can always spin up your containers again by doing `docker-compose up -d`.
- `docker-compose build` rebuilds your base image. example: you would do this if your `requirements.txt` changes
- `docker-compose ps` shows you the info about containers running including which ports they are listening on mapped to

To attach to one of your running containers, and run shell commands from within it:
```
$ `docker exec -it <container-name> /bin/bash`
```

This will take you to the workspace directory within the container. This directory should have your source code repository mounted so changes to source code on your local machine are reflected within the container. Once attached to the container, run arbitrary commands within that environment, like loading data to the database.

## Loading Data
For the final step before you can use the Broker, you will need to create a local admin user, and then initialize your database with data.

_NOTE: Many of the commands below use the format `docker exec -it dataact-broker-backend <cmd>`. You can alternatively run them by first attaching to the container with `docker exec -it dataact-broker-backend /bin/bash`, and then from within the container run `<cmd>` on the command line.

**Create a User**
```bash
$ docker exec -it dataact-broker-backend python dataactcore/scripts/initialize.py -a
```
This creates a local admin user that you can use to log in. The Broker utilizes MAX.gov for login when using a remote server, but we cannot recieve their response locally so we use a username and password for local development login. The administrative user created will utilize the credentials defined in your `config.yml` file.

Now try to browse to http://localhost:3000, and login with `valid.email@domain.com` and `password` (or whatever values you have configured in the `db.admin_email` and `db.admin_password` config params in `config.yml` or overridden in `local_config.yml`. You should get past the login screen to the home screen.

**Initialize Database**
```bash
$ docker exec -it dataact-broker-backend python dataactcore/scripts/initialize.py -i
```
This loads the information needed to validate data submissions: schemas, rules, and domain values such as object classes and account codes. You can view what each function does [here](https://github.com/fedspendingtransparency/data-act-broker-backend/blob/master/dataactcore/scripts/initialize.py), but the functions called by `initialize.py -i` are as follows:

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

_**Important Notes:**_
* By default, the broker installs with a small sample of [GTAS financial data](https://www.fiscal.treasury.gov/fsservices/gov/acctg/gtas/gtas_home.htm "GTAS"), which is used during the validation process. See the next section for more comprehensive options.

### Loading SF-133 data

If you'd like to install the broker using real GTAS data for your agency, replace the sample file with data representing the GTAS periods you want to validate against (using the same headers and data format as the sample file). The files should be named `dataactvalidator/config/sf_133_yyyy_mm.csv`, where `yyyy` is the fiscal year, and `mm` is the fiscal year period. This is only necessary for local installs.

If instead, you want to match the production environment (and are a developer on the DATA Act team), you can access our SF-133 files through S3. The data is sensitive, so we do not host it publicly. In the `prod-data-act-submission` bucket, within the `config` directory, you should see a series of `sf_133_yyyy_mm.csv` files. Download these and store them in your local `dataactvalidator/config` folder.

Once you've placed those files, run:
```bash
$ docker exec -it dataact-broker-backend python dataactvalidator/scripts/load_sf133.py
```

This will only load the new SF133 entries. To force load from your files, you can add the `-f` or `--force` flag:

```bash
$ docker exec -it dataact-broker-backend python dataactvalidator/scripts/load_sf133.py -f
```

This will take several minutes to process.

### CGAC, Object Class, and Program Activity data

CGAC file location: `dataactvalidator/config/cgac.csv`

Object Class file location: `dataactvalidator/config/object_class.csv`

Program Activity file location: `dataactvalidator/config/program_activity.csv`

To load these files:
```bash
$ docker exec -it dataact-broker-backend python dataactcore/scripts/initialize.py -d
```

### TAS data:

TAS file location: `dataactvalidator/config/cars_tas.csv`

To load TAS data:
```bash
$ docker exec -it dataact-broker-backend python dataactcore/scripts/initialize.py -t
```
