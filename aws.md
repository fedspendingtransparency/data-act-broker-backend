##AWS Setup
The DATA Act Core repository uses AWS for it's Simple Storage Service (S3), which
holds all files that are uploaded or created by the Core repository.

The configuration AWS file for the DAT Act core is `dataactcore/aws/s3bucket.json`.
It has the following format which defines the S3 bucket and S3 upload role.

```
{
  "bucket": "s3-bucket-name",
  "role": "ami-role"
}
```

The `bucket` parameter is the S3 storage space that the Core will use for files. The `role`
parameter is a AMI role that is used for uploading files.  The JSON file is created
during the local install process.

####Creating S3 Only Role

The DAT Act core repository allows for the creation of Simple Token Service (STS) tokens.  
These provide temporarily credentials to upload files to S3.  It is a best practice
to restrict the permissions to only file uploads. To do this, an  AMI Role
need to be created on the targeted AWS account. The role should have the following
permission JSON where the `s3-bucket-name` is the name of the S3 bucket.  

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

In addition to the permission JSON, the trust relationship needs to be updated
for the role. This allows an EC2 to assume the S3 uploading role when making
tokens.

####Credentials
For the cloud environment, it is a best practice to use AWS roles for EC2's
running the Core. AWS Roles provide a safe automated key management mechanism
to AWS resources. At a minimum, the role should be granted Full S3 access
permissions. Other repositories that use the core may have additional permissions
required.  

For local installs, credentials should be manage by the AWS CLI. This
process is part of the install guide which will walk users through the process.
