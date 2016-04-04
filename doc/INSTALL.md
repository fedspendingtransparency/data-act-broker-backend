# DATA Act Broker Installation and Setup

If you want to install and run a local DATA Act broker at your agency, the instructions below are for you. Because we designed the broker's components to be used together, the installation and setup directions for the DATA Act core, broker API, validator, and broker website are consolidated here.

**Note:** If you're a developer who wants to contribute to this code or modify it for use at your agency, your setup instructions are a little different. See the [contributing guide](CONTRIBUTING.md "DATA Act broker developer install") for more details.

## Install Backend Prerequisites

The core, the broker API, and the validator compose the backend of the DATA Act broker. These components are necessary for running the broker website, so we'll install them first.

To run the backend DATA Act broker applications, you'll need to have the following up and running on your machine:

* PostgreSQL
* DynamoDB
* Python

### PostgreSQL

[PostgreSQL](https://en.wikipedia.org/wiki/PostgreSQL) is an object-relational database management system (ORDBMS) with an emphasis on extensibility and standards-compliance.

You can find PostgreSQL installers on [EnterpriseDB](http://www.enterprisedb.com/products-services-training/pgdownload). Download and run the correct installer for your operating system (we recommend PostgreSQL 9.4.x.). As you proceed through the installation process, note your choices for port number, username, and password. You will need those when setting up the broker. For other PostgreSQL installation options, more complete documentation is available on the PostgreSQL [wiki](https://wiki.postgresql.org/wiki/Detailed_installation_guides).

**Note:** If you're setting up the broker on Mac OSX, we recommend using [homebrew](http://brew.sh) to install PostgreSQL.

## Create a Local DynamoDB

**Optional**

Note: If you'd prefer to use an Amazon-hosted version of DynamoDB, you can skip this section for installing DynamoDB on your machine.

Otherwise, you'll need to set up a local version of DynamoDB. DynamoDB local is a Java executable that runs on Windows, Mac, and Linux systems and is compatible with version 7+ of the Java Development Kit, which you will have to install as a first step: [http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html "download Java Development Kit").


After installing Java, [download the local DynamoDB files](http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.zip "download local DynamoDB") and follow [Amazon's instructions](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html "running DynamoDB on your computer") to run it.

Note that a local version of DynamoDB is **not** recommend for production.

Don't worry about creating tables in DynamoDB: the broker's [initialization process](#initialize-broker-backend-applications "initialize broker") handles that.

### Python and Virtualenv

The broker's backend components currently run on Python 2.7 (with the intention of migrating to Python 3x for the final release). Install Python 2.7 and pip (Python's package manager) using [these directions](https://docs.aws.amazon.com/cli/latest/userguide/installing.html#install-python "Install Python").

We highly recommend creating a [Python virtual environment](https://virtualenv.readthedocs.org/en/latest/installation.html "Python virtualenv") to house the DATA Act broker components. A virtual environment (aka *virtualenv*) will isolate the broker software and its libraries from the libraries running on your local system and prevent potential conflicts. In addition, we recommend using [virtualenvwrapper](https://virtualenvwrapper.readthedocs.org/en/latest/install.html "Python virtualenvwrapper documentation") to manage your Python environments.

1. Use pip to install virtualenv: `pip install virtualenv`
2. Use pip to install virtualenvwrapper: `pip install virtualenvwrapper`
3. If you're using Windows, also do `pip install virtualenvwrapper-powershell`.
3. Tell virtualenvwrapper where on your machine to create virtual environments and add it to your profile. This is a one-time virtualenvwrapper setup step, and the process varies by operating system. [This tutorial](http://newcoder.io/begin/setup-your-machine/ "Python: setting up your computer") covers setting up virtualenvwrapper on OSX, Linux, and Windows.
4. Create a virtual environment for the DATA Act software. In this example we've named the environment *data-act*, but you can call it anything: `mkvirtualenv data-act`.

    **Note:** If you're running multiple versions of Python on your machine, you can make sure your data act environment is running the correct Python version by pointing to a specific binary: `mkvirtualenv --python=[path to installed Python 2.7 executable] data-act`

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

## Create an S3 Bucket

**Optional**

The DATA Act broker can upload file submissions to an AWS S3 bucket if you so choose. If you're planning to store file submissions locally instead of using S3, skip to the [next section](#create-a-local-dynamodb "Create DynamoDB").

Assuming that you already have an AWS account, [create an AWS S3 bucket](http://docs.aws.amazon.com/AmazonS3/latest/gsg/CreatingABucket.html "Create a bucket") that will receive file submissions from the broker.

In order to support larger file uploads, you'll need to [modify the CORS settings](http://docs.aws.amazon.com/AmazonS3/latest/dev/cors.html#how-do-i-enable-cors) of your submission bucket with the following configuration:

```
<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <CORSRule>
        <AllowedOrigin>*</AllowedOrigin>
        <AllowedMethod>PUT</AllowedMethod>
        <AllowedMethod>GET</AllowedMethod>
        <AllowedMethod>POST</AllowedMethod>
        <MaxAgeSeconds>3000</MaxAgeSeconds>
        <AllowedHeader>*</AllowedHeader>
        <ExposeHeader>ETag</ExposeHeader>
    </CORSRule>
</CORSConfiguration>
```

## Create a Local DynamoDB

**Optional**

The broker tracks sessions in an Amazon DynamoDB table. If you'd like to use DynamoDB in the cloud with your AWS account, you can skip this section.

Otherwise, you'll need to set up a local version of DynamoDB. This requires Java JDK 6 or higher to be installed, which can be done using the following command on Red Hat based systems:


```bash
$ su -c "yum install java-1.7.0-openjdk"
```

For Ubuntu based systems the `apt-get` can be used instead

```bash
sudo apt-get install default-jre
```

Once Java is installed, you can download the local DynamoDB [here](http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.zip). Instructions to launch the local version once downloaded can be found in [AWS's User Guide](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html) along with the various options. Note that a local version of DynamoDB is **not** recommend for production.

Don't worry about creating tables in DynamoDB: the broker's [initialization process](#initialize-broker-backend-applications "initialize broker") handles that.

## Set Up AWS Permissions and Credentials

### Create S3 Only Role

When storing files on S3, it is a best practice to use AWS roles with the DATA Act broker. AWS roles provide a safe, automated key management mechanism to access and use AWS resources. At a minimum, this role should be granted Full S3 access permissions.

The DATA Act broker supports the creation of Security Token Service (STS) tokens that limit a user's permissions to only file uploads. To set this up, create an IAM Role on the targeted AWS account. This role should have the following permission JSON, where the `s3-bucket-name` is the name of the S3 bucket created above.

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

In addition to the permission JSON, create a Trust Relationship for the IAM role, allowing the broker to assume the S3 uploading role during token creation.

If not using a local Dynamo, the broker should also be granted read/write permissions to DynamoDB. The following JSON can be added to the role to grant this access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "dynamodb:*"
      ],
      "Effect": "Allow",
      "Resource": "arn:aws:dynamodb:REGION:ACCOUNT_ID:table/BrokerSession"
    }
  ]
}
```
The `REGION` should be replaced with region of the AWS account and the `ACCOUNT_ID` should be replaced with the AWS account ID.

### AWS Command Line Interface (CLI) Tools

AWS credentials should be managed by the AWS CLI.

Install the Python version of AWS Command Line Interface (CLI) tools following [these directions](https://docs.aws.amazon.com/cli/latest/userguide/installing.html#install-the-aws-cli-using-pip "install AWS CLI").

After you've installed the AWS CLI tools, configure them:

        $ aws configure

As prompted, enter the following information about your AWS account. Specify `json` as the default output format.

* `AWS Access Key ID`
* `AWS Secret Access Key`
* `Default region name [us-east-1]`
* `Default output format [json]`

## Install Broker Backend Applications

Make sure that your Python virtual environment is activated (replace *data-act* with the name of the environment you created above):

        $ workon data-act

Use Python's pip installer to install each broker back-end application:

        $ pip install git+git://github.com/fedspendingtransparency/data-act-core.git
        $ pip install git+git://github.com/fedspendingtransparency/data-act-broker.git
        $ pip install git+git://github.com/fedspendingtransparency/data-act-validator.git

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

* `Would you like to configure your logging`: `y`

    * `Would you like to log locally?`: '`y` (specify path when prompted)

* `Would you like to configure your database connection`: `y` if this is your first time running the initialization script or if you need re-create the broker databases for a version change, `n` otherwise (saying yes will delete any exising broker data)

    * `database address`: `localhost` to connect to your local Postgres instance
    * `database port`: `5432` is the Postgres default. If you specified a different post during your [Postgres install](https://github.com/fedspendingtransparency/data-act-core#dPostgreSQL "PostgreSQL install"), use that instead.
    * `database username`: the username created during your [Postgres install](https://github.com/fedspendingtransparency/data-act-core#dPostgreSQL "PostgreSQL install")
    * `database password`: the password for the username created during your [Postgres install](https://github.com/fedspendingtransparency/data-act-core#dPostgreSQL "PostgreSQL install")
    * `Default database name`: `postgres`

* `Would you like to configure your broker web API:` `y`
* `Would you like to install the broker locally`: `y`
    * `Enter the local folder used by the broker`: a path to the folder that will house "e-mails" sent by locally-installed versions of the broker
    * `Enter broker API port`: the local port you'll be running the broker on

* `Would you like to enable server side debugging`: `y` to turn on debug mode for the Flask server
* `Would you like to enable debug traces on REST requests`: `y` to provide debug output for REST responses
* `Enter the allowed origin (website that will allow for CORS)`: the URL that cross-origin HTTP requests are enabled on.
* `url` for the DATA Act validator: the url where you will be running the validator service
* `Enter system e-mail address`: the e-mail used by the broker for automated e-mails
* `Enter the port for the local dynamo database`: the local port used for your dynamo database (e.g., `8000`)
* `Enter the URL for the React application`: the URL where you'll be running the broker website
* `Enter application security key`: the key used to hash user passwords (should be a randomly-generated string)
* `Would you like to create the dynamo database table`: `y` if you're using AWS DynamoDB, `n` if you're running a local DynamoDB
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

To get the broker's website up and running, you'll need to install *Node.js* and a task runner called *Gulp.js*.

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
