import csv
from dataactcore.config import CONFIG_BROKER
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.responseException import ResponseException
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter


class CsvAbstractReader(object):
    """
    Reads data from S3 CSV file
    """

    BUFFER_SIZE = 8192
    header_report_headers = ["Error type", "Header name"]

    def open_file(self, region, bucket, filename, csv_schema, bucket_name, error_filename, long_to_short_dict):
        """ Opens file and prepares to read each record, mapping entries to specified column names
        Args:
            region: AWS region where the bucket is located (not used if instantiated as CsvLocalReader)
            bucket: the S3 Bucket (not used if instantiated as CsvLocalReader)
            filename: The file path for the CSV file in S3
            csv_schema: list of FileColumn objects for this file type
            bucket_name: bucket to send errors to
            error_filename: filename for error report
            long_to_short_dict: mapping of long to short schema column names
        """

        self.filename = filename
        self.unprocessed = ''
        self.extra_line = False
        self.lines = []
        self.packet_counter = 0
        self.is_finished = False
        self.column_count = 0
        header_line = self._get_line()
        # make sure we have not finished reading the file

        if self.is_finished:
            # Write header error for no header row
            with self.get_writer(bucket_name, error_filename, ["Error Type"], self.is_local) as writer:
                writer.write(["No header row"])
                writer.finishBatch()
            raise ResponseException("CSV file must have a header", StatusCode.CLIENT_ERROR,
                                    ValueError, ValidationError.singleRow)

        # create the header
        self.set_csv_delimiter(header_line, bucket_name, error_filename)

        header_row = next(csv.reader([header_line], dialect='excel',
                                     delimiter=self.delimiter))
        long_headers = use_long_headers(header_row, long_to_short_dict)
        header_row = list(normalize_headers(
            header_row, long_headers, long_to_short_dict))

        expected_header_counts = self.count_and_set_headers(
            csv_schema, header_row)

        self.column_count = len(header_row)

        self.handle_missing_duplicate_headers(
            expected_header_counts, bucket_name, error_filename)

        return long_headers

    @staticmethod
    def get_writer(bucket_name, filename, header, is_local, region=None):
        """
        Gets the write type based on if its a local install or not.
        """
        if is_local:
            return CsvLocalWriter(filename, header)
        if region is None:
            region = CONFIG_BROKER["aws_region"]
        return CsvS3Writer(region, bucket_name, filename, header)

    def get_next_record(self):
        """
        Read the next record into a dict and return it
        Returns:
            dictionary representing this record
        """
        return_dict = {}
        flex_dict = {}
        line = self._get_line()

        for row in csv.reader([line], dialect='excel', delimiter=self.delimiter):
            if len(row) != self.column_count:
                raise ResponseException("Wrong number of fields in this row", StatusCode.CLIENT_ERROR, ValueError,
                                        ValidationError.readError)
            for current, cell in enumerate(row):
                if current >= self.column_count:
                    raise ResponseException("Record contains too many fields", StatusCode.CLIENT_ERROR, ValueError,
                                            ValidationError.readError)
                if cell == "":
                    # Use None instead of empty strings for sqlalchemy
                    cell = None
                # self.expected_headers uses the short, machine-readable column names
                if self.expected_headers[current] is None:
                    if self.flex_headers[current] is not None:
                        flex_dict["header"] = self.flex_headers[current]
                        flex_dict["cell"] = cell
                    else:
                        # Skip this column as it is unknown or flex
                        continue
                else:
                    return_dict[self.expected_headers[current]] = cell
        return return_dict, flex_dict

    def close(self):
        """
        closes the file
        """
        raise NotImplementedError("Do not instantiate csvAbstractReader directly.")

    def _get_file_size(self):
        """
        Gets the size of the file
        """
        raise NotImplementedError("Do not instantiate csvAbstractReader directly.")

    def _get_next_packet(self):
        """
        Gets the next packet from the file returns true if successful
        """
        raise NotImplementedError("Do not instantiate csvAbstractReader directly.")

    def _get_line(self):
        """
        This method reads 8192 bytes from S3 Bucket at a time and stores
        it in a line buffer. The line buffer is used until its empty then
        another request is created to S3 for more data.
        """
        if len(self.lines) > 0:
            # Get the next line
            return self.lines.pop(0)
        # packets are 8192 bytes in size
        # for packet in self.s3File :
        while self.packet_counter * CsvAbstractReader.BUFFER_SIZE <= self._get_file_size():

            success, packet = self._get_next_packet()
            if not success:
                break
            self.packet_counter += 1

            # Get the current lines
            current_bytes = self.unprocessed + packet
            self.lines = self._split_lines(current_bytes)

            # edge case if the packet was filled with newlines only try again
            if len(self.lines) == 0:
                continue

            # last line still needs processing save and reuse
            self.unprocessed = self.lines.pop()
            if len(self.lines) > 0:
                # Get the next line
                return self.lines.pop(0)
        self.is_finished = True

        if len(self.unprocessed) < 5:
            # Got an extra line from a line break on the last line
            self.extra_line = True
        return self.unprocessed

    def _split_lines(self, packet):
        """
        arguments :
        packet unprocessed string of CSV data
        returns a list of strings broken by newline
        """
        lines_to_return = []
        escape_mode = False
        current = ""

        for index, char in enumerate(packet):
            if not escape_mode:
                if char == '\r' or char == '\n' or char == '\r\n':
                    if len(current) > 0:
                        lines_to_return.append(current)
                        # check the last char if its a new line add extra line
                        # as its at the end of the packet
                    if index == len(packet) - 1:
                        lines_to_return.append("")
                    current = ""
                else:
                    current = "".join([current, char])
                    if char == '"':
                        escape_mode = True
            else:
                if char == '"':
                    escape_mode = False
                current = "".join([current, char])
        if len(current) > 0:
            lines_to_return.append(current)
        return lines_to_return

    def set_csv_delimiter(self, header_line, bucket_name, error_filename):
        """Try to determine the delimiter type, raising exceptions if we
        cannot figure it out."""
        pipe_count = header_line.count("|")
        comma_count = header_line.count(",")

        if pipe_count != 0 and comma_count != 0:
            # Write header error for mixed delimiter use
            with self.get_writer(bucket_name, error_filename, ["Error Type"],
                                 self.is_local) as writer:
                writer.write(["Cannot use both ',' and '|' as delimiters. "
                              "Please choose one."])
                writer.finishBatch()
            raise ResponseException(
                "Error in header row: CSV file must use only '|' or ',' "
                "as the delimiter", StatusCode.CLIENT_ERROR, ValueError,
                ValidationError.headerError
            )

        self.delimiter = "|" if header_line.count("|") != 0 else ","

    def handle_missing_duplicate_headers(self, expected_fields, bucket_name,
                                         error_filename):
        """Check for missing or duplicated headers. If present, raise an
        exceptions with a meaningful message"""
        missing_headers = [cell for cell, count in expected_fields.items()
                           if count == 0]
        duplicated_headers = [cell for cell, count in expected_fields.items()
                              if count > 1]

        if missing_headers or duplicated_headers:
            self.write_missing_duplicated_headers(
                missing_headers, duplicated_headers, bucket_name,
                error_filename
            )
            raise_missing_duplicated_exception(
                missing_headers, duplicated_headers)

    def write_missing_duplicated_headers(
            self, missing_headers, duplicated_headers, bucket_name,
            error_filename):
        """Write header errors if any occurred and raise a header_error
        exception"""
        with self.get_writer(
                bucket_name, error_filename, self.header_report_headers,
                self.is_local) as writer:
            for header in duplicated_headers:
                writer.write(["Duplicated header", header])
            for header in missing_headers:
                writer.write(["Missing header", header])
            writer.finishBatch()

    def count_and_set_headers(self, csv_schema, header_row):
        """Track how many times we've seen a field we were expecting and set
        self.expected_headers and self.flex_headers"""
        self.expected_headers = []
        self.flex_headers = []

        # Track how many times we've seen a field we were expecting. Keyed by
        # the shorter, machine-readable column names
        expected_fields = {}

        for schema in csv_schema:
            expected_fields[FieldCleaner.cleanString(schema.name_short)] = 0

        for header_value in header_row:
            if header_value not in expected_fields:
                # Add flex headers to flex list
                if str(header_value).startswith("flex_"):
                    self.flex_headers.append(header_value)
                else:
                    self.flex_headers.append(None)
                # Allow unexpected headers, just mark the header as None so we
                # skip it when reading
                self.expected_headers.append(None)
            else:
                self.flex_headers.append(None)
                self.expected_headers.append(header_value)
                expected_fields[header_value] += 1
        return expected_fields


def use_long_headers(header_row, long_to_short_dict):
    """Check to see if header contains long or short column names"""
    col_matches = 0
    for value in header_row:
        if FieldCleaner.cleanString(value) in long_to_short_dict:
            col_matches += 1
    # if most of column headers are in the long format,
    # we'll treat the file as having long headers
    return col_matches > .5 * len(header_row)


def normalize_headers(header_row, long_headers, long_to_short_dict):
    for header in header_row:
        header = FieldCleaner.cleanString(header)
        if long_headers and header in long_to_short_dict:
            yield FieldCleaner.cleanString(long_to_short_dict[header])
        elif long_headers:
            yield None
        else:
            yield header


def raise_missing_duplicated_exception(missing_headers, duplicated_headers):
    """Construct and raise an exception about missing and/or duplicated
    headers"""
    error_string, extra_info = '', {}
    duplicated_str = ', '.join(duplicated_headers)
    missing_str = ', '.join(missing_headers)
    if duplicated_str:
        error_string = "Duplicated: " + duplicated_str
        extra_info['duplicated_headers'] = duplicated_str
    if missing_str:
        error_string = "Missing: " + missing_str
        extra_info['missing_headers'] = missing_str

    if error_string:
        raise ResponseException(
            "Errors in header row: " + str(error_string),
            StatusCode.CLIENT_ERROR,
            ValueError,
            ValidationError.headerError,
            **extra_info
        )
