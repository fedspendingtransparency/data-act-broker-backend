import io
import csv

class CsvAbstractWriter(object):
    """
    Writes a CSV to a Files System in a steaming manner
    use with the "with" python construct
    """


    BUFFER_SIZE =  (5 * 1024 ** 2)
    BATCH_SIZE = 100

    def __init__(self,filename,header) :
        """

        args

        filename - string filename and path in the S3 bucket
        header - list of strings for the header

        """
        self.rows = []
        self.write(header)

    def write(self,dataList) :
        """

        args
        dataList - list of strings to be written to a file system.
        Adds a row of csv into the S3 stream

        """
        byteList = []
        for data in dataList:
            if data is None:
                data = ""
            byteList.append(str(data).encode("UTF-8"))
        self.rows.append(byteList)
        if(len(self.rows) > self.BATCH_SIZE):
            ioStream = io.BytesIO()
            csvFormatter = csv.writer(ioStream)
            csvFormatter.writerows(self.rows)
            self._write(ioStream.getvalue())
            self.rows = []

    def finishBatch(self):
        """ Write the last unfinished batch """
        ioStream = io.BytesIO()
        csvFormatter = csv.writer(ioStream)
        csvFormatter.writerows(self.rows)
        self._write(ioStream.getvalue())
        self.rows = []

    def _write(self,data):
        """

        args

        data -  (string) a string be written to the current file

        """
        raise NotImplementedError("Do not instantiate csvAbstractWriter directly.")

    def __enter__(self) :
        return self

    def __exit__(self, type, value, traceback) :
        """

        args
        type - the type of error
        value - the value of the error
        traceback - the traceback of the error

        """
        raise NotImplementedError("Do not instantiate csvAbstractWriter directly.")
