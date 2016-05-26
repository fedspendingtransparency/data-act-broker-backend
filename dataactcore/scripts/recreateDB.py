from dataactcore.scripts.databaseSetup import dropDatabase
from dataactcore.scripts.setupErrorDB import setupErrorDB
from dataactcore.scripts.setupUserDB import setupUserDB
from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
import dataactcore.config

def recreate(dbList):
    """Drop and re-init specific databases. If dbList is 'all', do everything."""
    if dbList == 'all':
        list = []
        # get list of databases to recreate based on current config settings
        list.append(dataactcore.config.CONFIG_DB['user_db_name'])
        list.append(dataactcore.config.CONFIG_DB['job_db_name'])
        list.append(dataactcore.config.CONFIG_DB['error_db_name'])
        # TODO: add validator after the repo merge
    else:
        # TODO: if this script gets used a lot, put in some error-checking
        list = dbList

    for db in list:
        print ('Recreating {}'.format(db))
        dropDatabase(db)
        if 'error' in db:
            dataactcore.config.CONFIG_DB['error_db_name'] = db
            setupErrorDB()
        if 'user' in db:
            dataactcore.config.CONFIG_DB['user_db_name'] = db
            setupUserDB()
        if 'job' in db:
            dataactcore.config.CONFIG_DB['job_db_name'] = db
            setupJobTrackerDB()
