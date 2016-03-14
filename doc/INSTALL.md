# DATA Act Broker Installation and Setup

**TODO:** Test this on Windows

If you want to install and run a local DATA Act broker at your agency, the instructions below are for you. Because we designed the broker's components to be used together, the installation and setup directions for the DATA Act core, broker API, validator, and broker website are consolidated here.

**Note:** If you're a developer who wants to contribute to this code or modify it for use at your agency, your setup instructions are a little different. See the [developer-specific install guide](DEVELOPMENT.md "DATA Act broker developer install") for more details.

## Install Backend Prerequisites

The core, the broker API, and the validator compose the backend of the DATA Act broker. These components are necessary for running the broker website, so we'll install them first.

To run the backend DATA Act broker applications, you'll need to have the following up and running on your machine:

* PostgreSQL
* Python

### PostgreSQL

[PostgreSQL](https://en.wikipedia.org/wiki/PostgreSQL) is an object-relational database management system (ORDBMS) with an emphasis on extensibility and standards-compliance.

You can find PostgreSQL installers on [EnterpriseDB](http://www.enterprisedb.com/products-services-training/pgdownload). Download and run the correct installer for your operating system (we recommend PostgreSQL 9.4.x.). As you proceed through the installation process, note your choices for port number, username, and password. You will need those when setting up the broker. For other PostgreSQL installation options, more complete documentation is available on the PostgreSQL [wiki](https://wiki.postgresql.org/wiki/Detailed_installation_guides).

**Note:** If you're setting up the broker on Mac OSX, we recommend using [homebrew](http://brew.sh) to install PostgreSQL.

### Python and Virtualenv

The broker's backend components currently run on Python 2.7 (with the intention of migrating to Python 3x for the final release). Install Python 2.7 and pip (Python's package manager) using [these directions](https://docs.aws.amazon.com/cli/latest/userguide/installing.html#install-python "Install Python").

We highly recommend creating a [Python virtual environment](https://virtualenv.readthedocs.org/en/latest/installation.html "Python virtualenv") to house the DATA Act broker components. A virtual environment (aka *virtualenv*) will isolate the broker software and its libraries from the libraries running on your local system and prevent potential conflicts. In addition, we recommend using [virtualenvwrapper](https://virtualenvwrapper.readthedocs.org/en/latest/install.html "Python virtualenvwrapper documentation") to manage your Python environments.

1. Use pip to install virtualenv: `pip install virtualenv`
2. Use pip to install virtualenvwrapper: `pip install virtualenvwrapper` (*Windows users* should run `pip install virtualenvwrapper-win` instead).
3. **TODO:** instructions for setting WORKON_HOME, PROJECT_HOME and other dotfile junk you have to set up for virtualenvwrapper.
4. Create a virtual environment for the DATA Act software. In this example we've named the environment *data-act*, but you can call it anything: `mkvirtualenv data-act`.

    **Note:** If you're running multiple versions of Python on your machine, you can make sure your data act environment is running the correct Python version by pointed to a specific library: `mkvirtualenv --python=[path to installed Python 2.7 executable] data-act`

5. You should see some output that looks similar to the example below. Essentially, this command creates and activates a new virtualenv named `data-act` with its own set of Python libraries.  Anything you pip install from this point forward will be installed into the *data-act* environment rather than your machine's global Python environment. Your command line prompt indicates which (if any) virtualenv is active.

        rebeccasweger@GSA-xs-MacBook-Pro-4 ~
        $ mkvirtualenv --python=/usr/local/bin/python2.7 data-act-test
        Running virtualenv with interpreter /usr/local/bin/python2.7
        New python executable in data-act-test/bin/python2.7
        Also creating executable in data-act-test/bin/python
        Installing setuptools, pip...done.

        (data-act-test)
        rebeccasweger@GSA-xs-MacBook-Pro-4 ~

6. This new environment will be active until you run the `deactivate` command. You can re-activate the environment again at any time by typing `workon data-act`.


## Create and Configure Amazon Web Services (Optional)

You can optionally configure the DATA Act broker to upload files to an Amazon Web Services (AWS) S3 bucket. If you're planning to run the broker without using an S3 bucket, skip the sections below and go right to [Installing Broker Backend Applications](INSTALL.md#install-broker-backend-applications "Installing the broker backend apps").

**TODO:** Are there additional instructions needed for people running w/o an S3 bucket?**

The instructions below assume that you already have an AWS account and have created an S3 bucket to use for DATA Act submissions.

### Create S3 Only Role

The DATA Act broker supports the creation of Security Token Service (STS) tokens that only limit a user's permissions to file uploads. To set this up, create an IAM Role on the targeted AWS account. This role should have the following permission JSON, where the `s3-bucket-name` is the name of the S3 bucket:

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

In addition to the permission JSON, a create a Trust Relationship for the target role, allowing the EC2 instance to assume the S3 uploading role during token creation.


### AWS Credentials

For the cloud environment, it is a best practice to use AWS roles for any EC2 instance running the DATA Act broker. AWS roles provide a safe, automated key management mechanism to access and use AWS resources. At a minimum, the EC2 role should be granted Full S3 access permissions. Other repositories that use the core may need additional permissions.

**TODO:** will fully local broker installs need the AWS CLI tools as stated below?

For local broker installations, credentials should be managed by the AWS CLI. This process is part of the install guide, which will walk users through the process.

### Setup AWS Command Line Interface (CLI) Tools

Install the Python version of AWS Command Line Interface (CLI) tools following [these directions](https://docs.aws.amazon.com/cli/latest/userguide/installing.html#install-the-aws-cli-using-pip "install AWS CLI").

After you've installed the AWS CLI tools, configure them:

        $ aws configure

When prompted, enter:

**TODO: fill these in**

* xxxxx for the `AWS Access Key ID`
* xxxxx for the `AWS Secret Access Key`
* xxxxx for the `Default regionn name [us-east-1]`
* xxxxx for the `Default output format [json]`

## Install Broker Backend Applications

Make sure that your Python virtual environment is activated (replace *data-act* with the name of the environment you created above):

        $ workon data-act

Use Python's pip installer to install each broker back-end application:

        $ pip install git+git://github.com/fedspendingtransparency/data-act-core.git@development
        $ pip install git+git://github.com/fedspendingtransparency/data-act-broker.git@dev
        $ pip install git+git://github.com/fedspendingtransparency/data-act-validator.git@development

## Initialize Broker Backend Applications

If everything installed correctly in the steps above, you're ready to initialize the broker backend applications. To do this, you'll run a few scripts that set up the databases and create the configuration files.

**Note:** The scripts below delete existing broker databases and data. DO NOT run them in a production environment.

Run the broker API initialization script:

        $ sudo webbroker -i
        [when prompted, ented your sudo password]

You will now answer a series of questions to guide you through broker configuration. Below is some guidance, but the answers may vary depending on your specific environment:

* `Would you like to configure your S3 connection`: `y` if you're setting up an Amazon Web Service S3 bucket to use for broker file submission, `n` if you're running everything locally with no cloud dependencies

    * `Enter you bucket name`: the name of the S3 bucket you're using for broker file submission
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
* `Enter system e-mail address`: **TODO:** what is this?
* `Would you like to use a local dynamo database`: `n`
* `Enter the URL for the React application`: the URL where you'll be running the broker website
* `Enter application security key`: the key to use when creating e-mail tokens
* `Would you like to create the dynamo database table`: `n`
* `Would you like to configure the connection to the DATA Act validator`: `y`

    * `Enter url`: the full URL used for accessing the validator service (e.g., `http://locahost:3334`)

* `Would you like to configure the users to the DATA Act web api?`: `y`
* `Would you like to include test case users?`: `y`

    * `Enter the admin user password:` password for the broker's test admin username
    * `Enter the admin user e-mail`: e-mail address for the broker's test admin username

When you're done, you should be a message saying that the broker has been initialized. If you want to change any of the options, you can run `sudo webroker -i` again.

Run the validator initialization script:

        $ sudo validator -i
        [when prompted, ented your sudo password]

Now you'll answer some questions about the validator configuration. Below is guidance for some of the questions, but the answers may vary depending on your specific environment:

* `Would you like to configure your database connection?`: `n` (you did this during the broker configuration)
* `Would you like to configure your validator web service?`: `y`
* `Enter web service port`: the port number you'd like to use when running the validator service
* `Would you like to enable server side debugging?`: `y`
* `Would you like to enable debug traces on REST requests?`: `y`
* `Would you like to configure your appropriations rules?`: `y`
* `Enter the full file path for your schema (appropriationsFields.csv)`: [site-packages folder of your virtual environment]/dataactvalidator/scripts/appropriationsFields.csv
* `Enter the full file path for your rules (appropriationsRules.csv)`: [site-packages folder of your virtual environment]/dataactvalidator/scripts/appropriationsRules.csv
* `Would you like to add a new TAS File?`: `y`
* `Enter the full file path for your TAS data (all_tas_betc.csv)`: [site-packages folder of your virtual environment]/dataactvalidator/scripts/all_tas_betc.csv

The validator will now initialize, which can take several minutes as the script inserts validator rules into the database.

## Run Broker Backend Applications

After you've initialized the broker API and validator, start the broker API:

        $ webbroker -s

Make sure the broker API is working by visiting the URL specified when you initializing the application. For example, if you're running the broker on localhost port 3333, visit `http://localhost:3333` in your browser. You should see the message `Broker is running`.

**Pro-tip:** If you forget the URL and port you entered when setting up the application, check the config file located here:  `data-act-broker/dataactbroker/config/web_api_configuration.json`.

The process for starting the validator is similar:

        $ validator -s

Make sure the validator is working by visiting the URL and port you specified when initializing the application. For example, if you're running the validator on localhost port 3334, visit `http://localhost:3334` in your browser. You should see the message `Validator is running`.

**Pro-tip:** If you forget the validator URL and port you entered when setting up the validator, check the config file located here:  `data-act-validator/dataactvalidator/validator_configuration.json`. The `host` and `port` fields contain this information.

## Install Website Tools and Code

To the broker's website up and running, you'll need to install *Node.js* and a task runner called *Gulp.js*.

1. Download and run the Node.js installer for your operating system.
2. Use *npm*, Node's package manager to install the latest version of gulp (you'll need the latest version to ensure it can run the project's babel version of the `gulpfile`.):

        $ npm install gulp && npm install gulp -g

3. Get a copy of the website code to your local machine:

        $ git clone https://github.com/fedspendingtransparency/data-act-broker-web-app.git

4. There should now be a directory on your local machine called `data-act-broker-web-app`. Change to that directory:

        $ cd data-act-broker-web-app

## Start DATA Act Broker Website

Open the `GlobalConstants_dev.js` file in the `data-act-broker-app` directory. Change the value for `API` to the URL where you're running the broker (for example, `http://localhost:3333/v1`).

Run the following commands to install the website's dependencies and run the site:

        $ npm install
        $ gulp

Your browser will open to the broker webiste's login page. Log in with the admin account you created during the initialization process.

**TODO:** Test this
