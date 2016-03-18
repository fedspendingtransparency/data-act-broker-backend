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

If you're a developer, your DATA Act broker setup and install directions are slightly different than those in the main [install guide](INSTALL.md "DATA Act broker setup and install").

These directions assume that you will be developing 100% locally (*i.e.*, no dependencies on AWS).

### Install Backend Prerequisites

Follow the [directions for installing the broker's back-end prerequisites](INSTALL.md#install-backend-prerequisites "install back-end prerequisites").

### Clone Backend Application Code Repositories

Get the code and install the libraries for each backend piece of the DATA Act broker. These directions assume that you have [git setup](https://help.github.com/articles/set-up-git/ "set up git") on your machine and have a Github account with [ssh keys](https://help.github.com/articles/generating-an-ssh-key/ "generating an SSH key").

Before starting, make sure that your Python virtual environment is activated:

        $ workon your-virtual-environment

#### DATA Act Core

Navigate to wherever you install code on your machine (*e.g.*, ~/Dev/). From the command line, clone the DATA Act Core repository from GitHub to your local environment:

        $ git clone git@github.com:fedspendingtransparency/data-act-core.git

Navigate to the project's main folder:

        $ cd data-act-core

Use pip to install the Python libraries needed by the project:

        $ pip install -r requirements.txt

#### Broker API

Navigate back to your code directory. From the command line, clone the DATA Act Broker API repository from GitHub to your local environment:

        $ git clone git@github.com:fedspendingtransparency/data-act-broker.git

Navigate to the project's main folder:

        $ cd data-act-broker

Use pip to install the Python libraries needed by the project:

        $ pip install -r requirements.txt

#### Validator

Navigate back to your code directory. From the command line, clone the DATA Act Validator repository from GitHub to your local environment:

        $ git clone git@github.com:fedspendingtransparency/data-act-validator.git

Navigate to the project's main folder:

        $ cd data-act-validator

Use pip to install the Python libraries needed by the project:

        $ pip install -r requirements.txt

### Update $PYTHONPATH

The backend components import Python modules from one another. Therefore, the locations of these modules need to be on your $PYTHONPATH. Use the virtualenvwrapper [add2virtual](http://virtualenvwrapper.readthedocs.org/en/latest/command_ref.html#path-management "virtualenvwrapper path management") shortcut to add them:

        $ add2virtualenv [location of your code]/data-act-core
        $ add2virtualenv [location of your code]/data-act-broker
        $ add2virtualenv [location of your code]/data-act-validator

### Initialize Broker Backend Applications

Follow the [regular install directions](INSTALL.md#initialize-broker-backend-applications "DATA Act broker install") for initializing the broker, replacing the two initialization commands as follows:

* Instead of `sudo webbroker -i`, navigate to the `data-act-broker` directory and type `python dataactbroker/scripts/initialize.py -i`.
* Instead of `sudo validator -i`. navigate to the `data-act-validator` directory and type `python dataactvalidator/scripts/configure.py -i`

**Note:** when the validator setup script prompts you for appropriations rules full file paths, point to the files included in the validator's `scripts` folder (*i.e.*, `dataactvalidator/scripts`).

### Run Broker Backend Applications

After you've initialized the broker API and validator, start the broker API:

From the `data-act-broker` directory:

        $ python dataactbroker/app.py

Make sure the broker API is working by visiting the URL specified when you initializing the application. For example, if you're running the broker on localhost port 3333, visit `http://localhost:3333` in your browser. You should see the message `Broker is running`.

**Pro-tip:** If you forget the URL and port you entered when setting up the application, check the config file located here:  `data-act-broker/dataactbroker/config/web_api_configuration.json`.

The process for starting the validator is similar. From the `data-act-validator` directory:

        $ python dataactvalidator/app.py

Make sure the validator is working by visiting the URL and port you specified when initializing the application. For example, if you're running the validator on localhost port 3334, visit `http://localhost:3334` in your browser. You should see the message `Validator is running`.

**Pro-tip:** If you forget the validator URL and port you entered when setting up the validator, check the config file located here:  `data-act-validator/dataactvalidator/validator_configuration.json`. The `host` and `port` fields contain this information.

### Setup and Run Broker Website

Navigate back to your code directory. From the command line, clone the DATA Act web app repository from GitHub to your local environment:

        $ git clone git@github.com:fedspendingtransparency/data-act-broker-web-app.git

Navigate to the web app's main project directory:

        $ cd data-act-broker-web-app

To run the broker website, [follow the steps in the main broker install guide](INSTALL.md#start-data-act-broker-website "start the broker website").
