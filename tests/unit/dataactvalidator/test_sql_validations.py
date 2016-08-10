from random import randint
from dataactvalidator.interfaces.interfaceHolder import InterfaceHolder
from dataactcore.scripts.databaseSetup import dropDatabase
from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
from dataactcore.scripts.setupErrorDB import setupErrorDB
from dataactcore.scripts.setupValidationDB import setupValidationDB
from dataactcore.config import CONFIG_SERVICES, CONFIG_BROKER, CONFIG_DB
from dataactcore.scripts.databaseSetup import createDatabase,runMigrations
import dataactcore.config

class TestSqlValidations:

    def setup_class(self):
        """ Setup for class """

        # update application's db config options so unittests
        # run against test databases
        suite = self.__name__.lower()
        config = dataactcore.config.CONFIG_DB
        self.num = randint(1, 9999)
        config['db_name'] = 'unittest{}_{}_data_broker'.format(
            self.num, suite)
        dataactcore.config.CONFIG_DB = config
        createDatabase(CONFIG_DB['db_name'])
        runMigrations()

        # Allow us to augment default test failure msg w/ more detail
        self.longMessage = True
        # Flag for each route call to launch a new thread
        self.useThreads = False
        # Upload files to S3 (False = skip re-uploading on subsequent runs)
        self.uploadFiles = True
        # Run tests for local broker or not
        self.local = CONFIG_BROKER['local']
        # This needs to be set to the local directory for error reports if local is True
        self.local_file_directory = CONFIG_SERVICES['error_report_path']

        # drop and re-create test job db/tables
        setupJobTrackerDB()
        # drop and re-create test error db/tables
        setupErrorDB()
        # drop and re-create test validation db
        setupValidationDB()

        self.interfaces = InterfaceHolder()
        self.jobTracker = self.interfaces.jobDb
        self.stagingDb = self.interfaces.stagingDb
        self.errorInterface = self.interfaces.errorDb
        self.validationDb = self.interfaces.validationDb
        self.userId = 1

    def teardown_class(self):
        """ Teardown for class """
        self.interfaces.close()
        dropDatabase(self.interfaces.jobDb.dbName)