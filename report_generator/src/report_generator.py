"""
Report Generator - contains the classes to handle importing data from the source
files and manipulating for the JSON output.
"""

import csv
import os
import dask.dataframe as dd


class DataImporter:


    """
    Data Access Object - currently configured to read from .csv/.parquet files.
    Importing uses Dask in case datasets are too large to be kept in memory.
    """

    def __init__(self):

        self.supported_filetypes = ["csv", "parquet"]

    def import_file(self, file):

        """
        Check file extension to see which method to call.
        Raises Exception if file extension missing or unsupported.
        """

        if file.endswith(".csv"):
            return self.import_csv(file)
        if file.endswith(".parquet"):
            return self.import_parquet(file)

        raise UnsupportedFileTypeException("file name must end with file extension, "\
            f"supported file types are: {self.supported_filetypes}")

    @staticmethod
    def import_csv(file):

        """ Read csv file to Dask Dataframe."""

        with open(file, 'r', newline="", encoding="utf8") as csvfile:
            delimiter = csv.Sniffer().sniff(csvfile.read(1024)).delimiter
            csvfile.seek(0)
        csv_data = dd.read_csv(file, sep=delimiter)
        return csv_data

    @staticmethod
    def import_parquet(file):

        """ Read parquet file to Dask Dataframe."""

        parquet_data = dd.read_parquet(file, engine="pyarrow")
        # Using pyarrow engine above because fastparquet engine throws ParquetException

        return parquet_data


class DataTransformer:


    """
    Transformation Object - takes in two DataFrames and generates a JSON file.
    If there are shared column names, the filename can be appended to distinguish.
    """

    def __init__(self, df1, df2):

        self.df1 = df1
        self.df2 = df2
        # Default suffix values in case none are assigned
        self.left_suffix = "_1"
        self.right_suffix = "_2"

    def append_filenames_to_columns(self, file1, file2):

        """
        Set the suffixes to the filename (excluding extension).
        Should make differenciating overlapping columns easier.
        """

        filename1 = os.path.basename(os.path.splitext(file1)[0])
        filename2 = os.path.basename(os.path.splitext(file2)[0])

        self.left_suffix = "_" + filename1
        self.right_suffix = "_" + filename2

    def generate_json(self, join_columns, df1_select_columns, df2_select_columns):

        """
        Takes in two DataFrames, the columns to join on, and the columns to select
        and generates a JSON file
        """

        df_join = self.df1.join(self.df2.set_index(join_columns)
                            , how="left"
                            , on=join_columns
                            , lsuffix = self.left_suffix
                            , rsuffix = self.right_suffix)

        # Add suffixes to select columns that exist in both DataFrames
        # since that is how the joined DataFrame has them
        for col in df1_select_columns:
            # don't want to add suffix to join columns
            if (col in df2_select_columns) and (col not in join_columns) :
                df1_select_columns[df1_select_columns.index(col)] = col + self.left_suffix

        for col in df2_select_columns:
            # df1 column would already have the suffix added so need
            # to add left_suffix to our df2 column name to check for overlap
            if (col + self.left_suffix in df1_select_columns) and (col not in join_columns):
                df2_select_columns[df2_select_columns.index(col)] = col + self.right_suffix

        select_columns = df1_select_columns + df2_select_columns
        return df_join[select_columns].to_json(filename="report_*.json")

class UnsupportedFileTypeException(Exception):
    """Raised when file type is not supported by DataImporter class"""
