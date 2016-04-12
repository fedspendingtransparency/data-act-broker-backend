import boto
import csv
import os
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.responseException import ResponseException
from dataactvalidator.filestreaming.csvAbstractReader import CsvAbstractReader
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner

class CsvLocalReader(CsvAbstractReader):


    def openFile(self,region,bucket,filename,csvSchema,bucketName,errorFilename):
        """open file if it exists"""
        self.filename = filename
        self.isLocal = True
        try:
            self.file = open(filename,"r")
        except :
            raise ValueError("".join(["Filename provided not found : ",str(self.filename)]))
        super(CsvLocalReader,self).openFile(region,bucket,filename,csvSchema,bucketName,errorFilename)

    def close(self):
        """Closes file if it exists """
        try:
            self.file.close()
        except AttributeError:
            # File does not exist, and so does not need to be closed
            pass

    def _getFileSize(self):
        """
        Gets the size of the file
        """
        return os.path.getsize(self.filename)

    def _getNextPacket(self):
        """
        Gets the next packet from the file returns true if successful
        """
        packet  = self.file.read(CsvAbstractReader.BUFFER_SIZE).decode('utf-8')
        success = True
        if(packet == ""):
            success = False
        return success,packet
