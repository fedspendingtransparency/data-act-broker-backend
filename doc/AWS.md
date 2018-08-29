# DATA Act Broker Amazon Web Services (AWS) Setup

The directions below are optional when setting up the DATA Act Broker. You will only need to follow these instructions if you're planning to use AWS for broker file uploads, file storage, and session handling.

Assumptions:

* You already have an AWS account.

## Create an S3 Bucket

1. [Create an AWS S3 bucket](http://docs.aws.amazon.com/AmazonS3/latest/gsg/CreatingABucket.html "Create a bucket") that will receive file submissions from the broker.

2. [Modify the CORS settings](http://docs.aws.amazon.com/AmazonS3/latest/dev/cors.html#how-do-i-enable-cors) of your submission bucket with the following configuration.


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


## Set Up AWS Permissions and Credentials

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
