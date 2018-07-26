# DATA Act Broker Installation and Setup

The easiest way to run a local DATA Act broker at your agency is to use our Docker image. Essentially, this image is a packaged version of the broker that is pre-configured and will run in an isolated environment.

If you're a developer who wants to run the broker on your machine and make changes to the code, please see the install directions in the project's contributing guide: [CONTRIBUTING.md](CONTRIBUTING.md "project contributing guide").

### Create Broker Config Files

Before running the broker, you'll need to provide a few configuration options. The broker uses three config files:

* `config.yml`: Config parameters shared across broker instances (_e.g._, production, staging, development, local).
* `[instance]_config.yml`: Config parameters specific to a broker instance (_e.g._, location of the development database, url for the development broker instance). Values in this file override their corresponding values in `config.yml`.
* `[instance]_secrets.yml`: Sensitive config values specific to a broker instance (_e.g._, database passwords).

At startup, the broker looks for an environment variable called `env` and will use that to set the instance name. If there is no `env` environment variable, the broker will set the instance name to `local` and look for `local_config.yml` and `local_secrets.yml`.

There are sample config files in `data-act-broker-backend/dataactcore`. Use these as a starting point when setting up the broker. The instructions below assume that you're installing the broker for local development.
These are the list of the files to be copied and renamed.
```
dataactcore/config_example.yml
dataactcore/local_config_example.yml
dataactcore/local_secrets_example.yml
dataactvalidator/config/example_agency_codes_list.csv
dataactvalidator/config/example_cars_tas.csv
dataactvalidator/config/example_cgac.csv
dataactvalidator/config/example_object_class.csv
dataactvalidator/config/example_program_activity.csv
```
If you don't already have your own configs or don't want to use your local configs you can use this script to copy and rename all the necessary config files (run script from root level of this repo):
```
#!/bin/bash

cp dataactcore/config_example.yml dataactcore/config.yml
cp dataactcore/local_config_example.yml dataactcore/local_config.yml
cp dataactcore/local_secrets_example.yml dataactcore/local_secrets.yml


cp dataactvalidator/config/example_agency_codes_list.csv dataactvalidator/config/agency_codes_list.csv
cp dataactvalidator/config/example_cars_tas.csv dataactvalidator/config/cars_tas.csv
cp dataactvalidator/config/example_cgac.csv dataactvalidator/config/cgac.csv
cp dataactvalidator/config/example_object_class.csv dataactvalidator/config/object_class.csv
cp dataactvalidator/config/example_program_activity.csv dataactvalidator/config/program_activity.csv```
```

### Setup with Docker

Install docker in your local machine by selecting your OS and hitting install from this [link](https://docs.docker.com/install/) (this installation includes `docker-compose` as well).

Next step is to refer to the `Create Broker Config Files` section of this documentation to copy and rename config files, if you choose to use the default configs.

After you successfully installed Docker, make sure the docker daemon is running on your local machine by running `docker version` and make sure you have your configs renamed and copied. Run the following command in the root level of this backend repository:

- `docker-compose up -d`  This command will spin up the postgres container `dataact-postgres`, build your backend image `broker-backend` and run the two service `dataact-broker` and `dataact-validator`. This will also initialize the user database with username password located in the configs and it will load all the local agencies data into the database with `initialize -a` and `-i` options. This process will take longer the first time because it's building the image and installs the requirements. NOTE: remove `-d` option if you want to see docker logs, you can `contrl z` out of the logs anytime.

These commands are useful for debugging but is optional:

- `docker-compose build` rebuilds your base image. example: you would do this if your requirements.txt changes.

- `docker-compose down` shuts down all your local containers and removes them. This may help debug some problems. You can always spin up your containers again by doing `docker-compose up -d`.

- `docker ps` shows you the info about containers running on your local machines including which ports it's mapped to.

Wait about 30 seconds for everything to come up (first time setup can take up to 8 minutes). At this point you can go to your browser and hit the broker api by going to `http://127.0.0.1:9999/v1/current_user/`.

Run these commands to login/ssh to the broker and validator containers:

- `docker exec -it dataact-broker /bin/bash` login/ssh to broker.

- `docker exec -it dataact-validator /bin/bash` login/ssh to validator.

This will take you to the workspace directory within the dataact-broker and dataact-validator containers respectively, that will have your backend repository mounted so local changes in that repository will also be changed within the container.

#####set up with existing postgres

If you want to use postgres on your local machine, change the config to point to your host IP. If you are using docker version `17.06` and have a mac use `docker.for.mac.localhost` instead of IP. If you are using `17.12.0` substitute `docker.for.mac.host.internal` and for `18.03.0` and higher use `host.docker.internal` for your host IP to connect to your local machine.
