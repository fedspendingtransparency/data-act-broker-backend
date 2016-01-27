## The Data Act Core Repository

The Core Repository is a collection of common components used by other
Data Act Python repositories.  The structure for the repository is as follows.

```
dataactcore/
├── aws/            (Common AWS Functions)
├── credentials/    (Database Credentials)
├── models/         (ORM Models and Database Interfaces)
├── scripts/        (Database Setup Scripts)
└── utils/          (JSON Helper objects)
```

#### AWS

All common code that uses the AWS Boto SDK resides in the AWS folder. The AWS CLI needs to be installed to use Boto. This process is covered in the installation guide.

#### Models

The models folder contains the object-relational mapping (ORM) definition for all models used in the Data Act project. When a new table is needed the ORM needs to be updated. Additional fields exist on some of the models enable the auto population of foreign key relationships . These fields use the `relationship` function to signify a mapping to another table.

Database interfaces are defined for each database used within the project.
Each interface inherits base functionality from the `BaseInterface`.  Interfaces are extended in the other repositories to add additional functionality when required.

#### Scripts

The scripts folder contains various tools to setup the various JSON needed
to use the core. Scripts to create the database structure are also located
within. Additional data clearing scripts are also included.

#### Utils

In the utils folder common rest request and error handling objects are
located. These provide a common way for other repositories to handle
requests.
