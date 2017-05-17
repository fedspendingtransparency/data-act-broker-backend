import csv
import os
import tempfile

import boto3

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.stagingModels import FlexField
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner
from dataactvalidator.validation_handlers.validationError import ValidationError


class CsvReader(object):
    """
    Reads data from local CSV file (first downloading from S3 if necessary)
    """

    header_report_headers = ["Error type", "Header name"]

    def open_file(self, region, bucket, filename, csv_schema, bucket_name, error_filename, long_to_short_dict):
        """ Opens file and prepares to read each record, mapping entries to specified column names
        Args:
            region: AWS region where the bucket is located
            bucket: Optional parameter; if set, file will be retrieved from S3
            filename: The file path for the CSV file (local or in S3)
            csv_schema: list of FileColumn objects for this file type
            bucket_name: bucket to send errors to
            error_filename: filename for error report
            long_to_short_dict: mapping of long to short schema column names
        """

        self.has_tempfile = False
        # If this is a file in S3, download to a local temp file first
        if region and bucket:
            # Use temp file as local file

            filename = self._transfer_s3_file_to_local(region, bucket, filename)

        self.filename = filename
        self.is_local = True
        try:
            self.file = open(filename, "r")
        except:
            raise ValueError("".join(["Filename provided not found : ", str(self.filename)]))

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
                writer.finish_batch()
            raise ResponseException("CSV file must have a header", StatusCode.CLIENT_ERROR,
                                    ValueError, ValidationError.singleRow)

        # create the header
        self.set_csv_delimiter(header_line, bucket_name, error_filename)

        header_row = next(csv.reader([header_line], dialect='excel', delimiter=self.delimiter))
        long_headers = use_long_headers(header_row, long_to_short_dict)
        header_row = list(normalize_headers(header_row, long_headers, long_to_short_dict))

        expected_header_counts = self.count_and_set_headers(csv_schema, header_row)

        self.column_count = len(header_row)

        self.handle_missing_duplicate_headers(expected_header_counts, bucket_name, error_filename)

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

    def _transfer_s3_file_to_local(self, region, bucket, filename):
        # mkstemp returns a file handle and a path to the created file
        (file, file_path) = tempfile.mkstemp()
        self.has_tempfile = True

        s3 = boto3.client('s3', region_name=region)

        s3.download_file(bucket, filename, file_path)

        return file_path

    def get_next_record(self):
        """
        Read the next record into a dict and return it
        Returns:
            pair of (dictionary of expected fields, list of FlexFields)
        """
        return_dict = {}
        flex_fields = []
        line = self._get_line()

        row = next(csv.reader([line], dialect='excel', delimiter=self.delimiter))
        if len(row) != self.column_count:
            raise ResponseException(
                "Wrong number of fields in this row, expected %s got %s" %
                (self.column_count, len(row)), StatusCode.CLIENT_ERROR,
                ValueError, ValidationError.readError)
        for idx, cell in enumerate(row):
            if idx >= self.column_count:
                raise ResponseException(
                    "Record contains too many fields", StatusCode.CLIENT_ERROR,
                    ValueError, ValidationError.readError)
            # Use None instead of empty strings for sqlalchemy
            if cell == "":
                cell = None
            # self.expected_headers uses the short, machine-readable column
            # names
            if self.expected_headers[idx] is None and self.flex_headers[idx] is not None:
                flex_fields.append(FlexField(header=self.flex_headers[idx], cell=cell))
            # We skip headers which aren't expected and aren't flex
            elif self.expected_headers[idx] is not None:
                return_dict[self.expected_headers[idx]] = cell
        return return_dict, flex_fields

    def _get_line(self):
        line = self.file.readline()
        # Empty lines are represented with '\n'. Read until we get a non-empty line or get an empty string
        # signifying end of file.
        while line == '\n':
            line = self.file.readline()

        # If the line is empty, we've reached the end of the file.
        if not line:
            self.is_finished = True
            self.extra_line = True
        return line

    def set_csv_delimiter(self, header_line, bucket_name, error_filename):
        """Try to determine the delimiter type, raising exceptions if we
        cannot figure it out."""
        pipe_count = header_line.count("|")
        comma_count = header_line.count(",")

        if pipe_count != 0 and comma_count != 0:
            # Write header error for mixed delimiter use
            with self.get_writer(bucket_name, error_filename, ["Error Type"], self.is_local) as writer:
                writer.write(["Cannot use both ',' and '|' as delimiters. Please choose one."])
                writer.finish_batch()
            raise ResponseException(
                "Error in header row: CSV file must use only '|' or ',' as the delimiter", StatusCode.CLIENT_ERROR,
                ValueError, ValidationError.headerError
            )

        self.delimiter = "|" if header_line.count("|") != 0 else ","

    def handle_missing_duplicate_headers(self, expected_fields, bucket_name, error_filename):
        """Check for missing or duplicated headers. If present, raise an
        exceptions with a meaningful message"""
        missing_headers = [cell for cell, count in expected_fields.items() if count == 0]
        duplicated_headers = [cell for cell, count in expected_fields.items() if count > 1]

        if missing_headers or duplicated_headers:
            self.write_missing_duplicated_headers(
                missing_headers, duplicated_headers, bucket_name,
                error_filename
            )
            raise_missing_duplicated_exception(missing_headers, duplicated_headers)

    def write_missing_duplicated_headers(self, missing_headers, duplicated_headers, bucket_name, error_filename):
        """Write header errors if any occurred and raise a header_error
        exception"""
        with self.get_writer(bucket_name, error_filename, self.header_report_headers, self.is_local) as writer:
            for header in duplicated_headers:
                writer.write(["Duplicated header", header])
            for header in missing_headers:
                writer.write(["Missing header", header])
            writer.finish_batch()

    def count_and_set_headers(self, csv_schema, header_row):
        """Track how many times we've seen a field we were expecting and set
        self.expected_headers and self.flex_headers"""
        self.expected_headers = []
        self.flex_headers = []

        # Track how many times we've seen a field we were expecting. Keyed by
        # the shorter, machine-readable column names
        expected_fields = {}

        for schema in csv_schema:
            expected_fields[FieldCleaner.clean_string(schema.name_short)] = 0

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

    def close(self):
        """Closes file if it exists """
        try:
            self.file.close()
            if self.has_tempfile:
                os.remove(self.filename)
        except AttributeError:
            # File does not exist, and so does not need to be closed
            pass

    def _get_file_size(self):
        """
        Gets the size of the file
        """
        return os.path.getsize(self.filename)


def use_long_headers(header_row, long_to_short_dict):
    """Check to see if header contains long or short column names"""
    col_matches = 0
    for value in header_row:
        if FieldCleaner.clean_string(value) in long_to_short_dict:
            col_matches += 1
    # if most of column headers are in the long format,
    # we'll treat the file as having long headers
    return col_matches > .5 * len(header_row)


def normalize_headers(header_row, long_headers, long_to_short_dict):
    for header in header_row:
        header = FieldCleaner.clean_string(header)
        # Replace correctly spelled header (which does NOT match the db) with the
        # misspelling that DOES match the db
        if header == 'deobligationsrecoveriesrefundsofprioryearbyprogramobjectclass_cpe':
            header = 'deobligationsrecoveriesrefundsdofprioryearbyprogramobjectclass_cpe'
        if long_headers and header in long_to_short_dict:
            yield FieldCleaner.clean_string(long_to_short_dict[header])
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
