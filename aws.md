##AWS Setup
The DATA Act Core repository uses AWS Simple Storage Service (S3) to store all files.

The configuration AWS file for the DAT Act core is `dataactcore/aws/s3bucket.json`. It has the following format, which defines the S3 bucket and S3 upload role:

```
{
  "bucket": "s3-bucket-name",
  "role": "iam-role"
}
```

The `bucket` parameter is the S3 storage space that the Core will use for files. The `role` parameter is the IAM role that is used for uploading files.  The JSON file is created during the local install process.

####Creating S3 Only Role

The DATA Act Core repository allows for the creation of Security Token Service (STS) tokens that only allow for uploading files. To do this, an IAM Role needs to be created on the targeted AWS account. This role should have the following permission JSON, where the `s3-bucket-name` is the name of the S3 bucket:


```
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

In addition to the permission JSON, a Trust Relationship needs to be created for the target role, allowing the EC2 instance to assume the S3 uploading role during token creation.

####Credentials
For the cloud environment, it is a best practice to use AWS roles for any EC2 instance running the Core. AWS roles provide a safe, automated key management mechanism to access and utilize AWS resources. At a minimum, the EC2 role should be granted Full S3 access permissions. Other repositories that use the core may have additional permissions required.

For local installations, credentials should be managed by the AWS CLI. This
process is part of the install guide, which will walk users through the process.
