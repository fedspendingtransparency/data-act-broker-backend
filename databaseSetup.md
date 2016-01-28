## Data Act Data Broker Database Setup Guide

#### Requirements

Before beginning this process, the [data-act-core](https://github.com/fedspendingtransparency/data-act-core) and [data-act-validator](https://github.com/fedspendingtransparency/data-act-validator) repositories should be installed. Please see the Data Act Installation Guide for details on this process.

You will also need to have setup a Postgres database to be used by the broker. Information about this database should be placed in a JSON file in your data-act-core installation located at `dataactcore/credentials/dbCred.json`, containing a JSON dictionary with keys `username`, `password`, `host`, and `port`. Below is an example of what should be in this file: 

```json
{
    "username":"user",
    "password":"pass",
    "host":"localhost",
    "port":"5432"
}
```

#### Setup Scripts

After creating the Postgres database and credentials file, several setup scripts should be run to create the databases and tables that will be used by the broker. In your data-act-core installation, there will be a folder [dataactcore/scripts/](https://github.com/fedspendingtransparency/data-act-core/dataactcore/scripts). In this folder, run the following python scripts: `createJobTables.py`, `setupErrorDB.py`, `setupStaging.py`, and `setupValidationDB.py`.

Finally, to prepare the validator to run checks against a specified set of fields and rules, your data-act-validator installation will have a [scripts/](https://github.com/fedspendingtransparency/data-act-validator/scripts) folder containing `loadApprop.py` and `loadTas.py`. `loadApprop.py` may be run as is to create the rule set for testing an appropriations file, or you may replace `appropriationsFields.csv` and `appropriationsRules.csv` with custom versions to run a different set of rules.

For Treasury Account Symbol checks, you'll need to get the updated `all_tas_betc.csv` file from https://www.sam.fms.treas.gov/sampublic/tasbetc.htm. Choose the file from row "Total Files" and column "CSV" and place that in the [scripts/](https://github.com/fedspendingtransparency/data-act-validator/scripts) folder before running `loadTas.py`. Once both scripts have run, the databases will contain everything they need to validate appropriations files.

#### Data Broker Database Reference

After setup, there will be five databases created, this section will describe each of them.

* `error_data` - Holds file level errors in the `file_status` table, along with information about number of row level errors of each type in the `error_data` table. A complete list of every separate occurrence can be found in the error report csv file.
* `job_tracker` - Holds information on all validation and upload jobs, including status of jobs and relations between jobs. The `job_status` table is the main place to get this information, and provides file name and type, status of the job, what submission the job is part of, and what table the results can be found in. The `job_dependency` table details precedence constraints between jobs, giving the job IDs for both the prerequisite and the dependent job.
* `staging` - Holds records that passed validation. Each file validated will have a table in this database, named based on the job ID. If the `file_status` table in the `error_data` database lists the file as completed, then each record in the input file will be present in either this staging table or the error report.
* `user_manager` - Holds a mapping between user names and user IDs to be used for providing submission history information to a user.
* `validation` - Contains all the information a submitted file is validated against. The `file_columns` table details what columns are expected in each file type, and the rule table maps all defined single-field rules to one of the columns specified in `file_columns`. The `multi_field_rule` table stores rules that involve a set of fields, but are still checked against a single record at a time. Finally, the `tas_lookup` table holds the set of valid TAS combinations, taken from the TAS csv file discussed in the setup section.
