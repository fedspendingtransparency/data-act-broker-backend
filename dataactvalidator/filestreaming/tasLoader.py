import csv
from sqlalchemy.exc import IntegrityError
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface
from dataactvalidator.filestreaming.loaderUtils import LoaderUtils

class TASLoader(object):
    """ Loads valid TAS combinations from CARS file """

    FILE_SCHEMA = ["ATA","AID","BPOA","EPOA","A","MAIN","SUB"]
    PAD_LENGTH = {"ATA":3,"AID":3,"MAIN":4,"SUB":3}
    @classmethod
    def loadFields(cls,filename):
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
                for key in record:
                    if key not in cls.FILE_SCHEMA:
                        # Not one of the TAS fields
                        continue
                    record[key] = record[key].strip()
                    if record[key] == "":
                        # Set blanks to None
                        record[key] = None
                    if key in cls.PAD_LENGTH and record[key] is not None:
                        record[key] = record[key].zfill(cls.PAD_LENGTH[key])

                #Check if record exists
                if(not (LoaderUtils.compareRecords(record,lastRecord,TASLoader.FILE_SCHEMA))) :
                    if(LoaderUtils.checkRecord(record,TASLoader.FILE_SCHEMA)) :
                        try:
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
                        except IntegrityError as e:
                            # Hit a duplicate value that violates index, skip this one
                            totalExistingTAS += 1
                            print("".join(["Warning: Skipping this row: ",str(record)]))
                            print("".join(["Due to error: ",str(e)]))
                            database.session.rollback()
                    else :
                       raise ValueError('CSV File does not follow schema')
                else :
                    totalExistingTAS += 1
                lastRecord  = record
        #Step 3 Report Metrics for debuging
        print("".join(["Total TAS added : ",str(totalTASAdded)]))
        print("".join(["Duplicate TAS in file :",str(totalExistingTAS)]))
        print("".join(["Total TAS in file : ",str(totalExistingTAS + totalTASAdded)]))