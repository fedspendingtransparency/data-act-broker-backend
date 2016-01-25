import csv
from interfaces.validationInterface import ValidationInterface
class TASLoader(object):

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
        database = ValidationInterface()
        database.deleteTAS()
        counter  = 0
        lastRecord = {}
        #Step 2 add the new data
        with open(filename,'rU') as csvfile:
            reader = csv.DictReader(csvfile)
            for record in reader:
                counter = counter + 1
                #Let the user know that the script is still running.
                if(counter % 40000 == 0) :
                    print "Loading ... " + str(counter)
                #Pad Record
                record["ATA"] = record["ATA"].zfill(3)
                record["AID"] = record["AID"].zfill(3)
                record["BPOA"] = record["BPOA"].zfill(4)
                record["EPOA"] = record["EPOA"].zfill(4)
                record["A"] = record["A"].zfill(1)
                record["MAIN"] = record["MAIN"].zfill(4)
                record["SUB"] = record["SUB"].zfill(3)
                #Check if record exists
                if(not (TASLoader.compareRecords(record,lastRecord,TASLoader.FILE_SCHEMA))) :
                    if(TASLoader.checkRecord(record,TASLoader.FILE_SCHEMA)) :
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
        print "Total TAS added : " + str(totalTASAdded)
        print "Duplicate TAS in file :" + str(totalExistingTAS)
        print "Total TAS in file : " + str(totalExistingTAS + totalTASAdded)



    @staticmethod
    def compareRecords (recordA,recordB, fields) :
        """ Compares two dictionaries based of a field subset """
        for data in fields:
            if (  data in recordA and  data in recordB   ):
                if( not recordA[data]== recordB[data]) :
                    return False
            else :
                return False
        return True


    @staticmethod
    def checkRecord (record, fields) :
        """ Returns True if all elements of fields are present in record """
        for data in fields:
            if ( not data in record ):
                return False
        return True
