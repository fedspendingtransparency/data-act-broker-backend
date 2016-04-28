import csv
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface
from dataactvalidator.filestreaming.loaderUtils import LoaderUtils

class TASLoader(object):
    """ Loads valid TAS combinations from CARS file """

    FILE_SCHEMA = ["ATA","AID","BPOA","EPOA","A","MAIN","SUB"]
    @staticmethod
    def loadFields(filename):
        """
        Load schema file to create validation rules and removes existing
        schemas

        Arguments:
        filename -- filename of csv file that holds TAS data
        """
        totalTASAdded = 0
        totalExistingTAS = 0
        #Step 1 Clean out the database
        database = ValidatorValidationInterface()
        database.deleteTAS()
        lastRecord = {}
        #Step 2 add the new data
        with open(filename,'rU') as csvfile:
            #skip the first line of the csv as its just metadata
            next(csvfile, None)
            #second line contains headers
            reader = csv.DictReader(csvfile)
            #Loop over each row
            for index,record in enumerate(reader):
                #Let the user know that the script is still running.
                if(index % 40000 == 0) :
                    print("".join(["Loading ... ",str(index)]))
                #Pad Record
                record["ATA"] = record["ATA"].zfill(3)
                record["AID"] = record["AID"].zfill(3)
                record["BPOA"] = record["BPOA"].zfill(4)
                record["EPOA"] = record["EPOA"].zfill(4)
                record["A"] = record["A"].zfill(1)
                record["MAIN"] = record["MAIN"].zfill(4)
                record["SUB"] = record["SUB"].zfill(3)
                #Check if record exists
                if(not (LoaderUtils.compareRecords(record,lastRecord,TASLoader.FILE_SCHEMA))) :
                    if(LoaderUtils.checkRecord(record,TASLoader.FILE_SCHEMA)) :
                        if(database.addTAS(record["ATA"],
                            record["AID"],
                            record["BPOA"],
                            record["EPOA"],
                            record["A"],
                            record["MAIN"],
                            record["SUB"])) :
                            totalTASAdded += 1
                        else :
                            totalExistingTAS += 1
                    else :
                       raise ValueError('CSV File does not follow schema')
                else :
                    totalExistingTAS += 1
                lastRecord  = record
        #Step 3 Report Metrics for debuging
        print("".join(["Total TAS added : ",str(totalTASAdded)]))
        print("".join(["Duplicate TAS in file :",str(totalExistingTAS)]))
        print("".join(["Total TAS in file : ",str(totalExistingTAS + totalTASAdded)]))