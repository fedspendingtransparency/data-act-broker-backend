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

### Create S3 Only Role

When storing files on S3, it is a best practice to use AWS roles with the DATA Act broker. AWS roles provide a safe, automated key management mechanism to access and use AWS resources. At a minimum, this role should be granted Full S3 access permissions.

The DATA Act broker supports the creation of Security Token Service (STS) tokens that limit a user's permissions to only file uploads. To set this up, create an IAM Role on the targeted AWS account. This role should have the following permission JSON, where the `s3-bucket-name` is the name of the S3 bucket created above.

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

In addition to the permission JSON, create a Trust Relationship for the IAM role, allowing the broker to assume the S3 uploading role during token creation.

If not using a local Dynamo, the broker should also be granted read/write permissions to DynamoDB. The following JSON can be added to the role to grant this access:

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
