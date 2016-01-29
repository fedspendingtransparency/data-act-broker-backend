## The DATA Act Core Repository

The DATA Act Core repository is a collection of common components used by other
Data Act repositories.  The structure for the repository is as follows:

```
dataactcore/
├── aws/            (Common AWS functions)
├── credentials/    (Database credentials)
├── models/         (ORM models and database interfaces)
├── scripts/        (Database setup scripts)
└── utils/          (JSON helper objects)
```

#### AWS

The `aws/` folder contains all common code that uses the AWS Boto SDK. The AWS CLI needs to be installed to use Boto. This process is covered in the installation guide.

#### Models

The `models/` folder contains the object-relational mapping (ORM) definition for all models used in the DATA Act project. When a new table is needed the ORM needs to be updated. Additional fields exist on some of the models to enable the automatic population of foreign key relationships. These fields use the `relationship` function to signify a mapping to another table.

Database interfaces are defined for each database used within the project.
Each interface inherits base functionality from `BaseInterface`. Interfaces are extended in the other repositories to add additional functionality where required.

#### Scripts

The `scripts/` folder contains various tools to setup the various JSON needed
to use the Core. Scripts to create the database structure are also located
within. Additional data clearing scripts are also included.

#### Utils

The `utils/` folder contains common REST requests and error handling objects.
These provide a common way for other repositories to handle requests.
