import os
import pytest
import dask.dataframe as dd
import unittest.mock
import pandas
import json
from pytest_mock import mocker
from csv import reader
from unittest.mock import patch, mock_open
from src.report_generator import DataImporter, DataTransformer, UnsupportedFileTypeException


@pytest.fixture()
def importer():
    yield DataImporter()


@pytest.fixture()
def transformer():
    test_data1 = {"Id": ["1", "2", "3"]
        , "Name": ["John", "Steve", "Alice"]}

    test_data2 = {"Id": ["1", "2", "3"]
    , "Email": ["john@mail.com", "steve@mail.com", "alice@mail.com"]}

    pdf1 = pandas.DataFrame(test_data1)
    pdf2 = pandas.DataFrame(test_data2)
    ddf1 = dd.from_pandas(pdf1, npartitions=1)
    ddf2 = dd.from_pandas(pdf2, npartitions=1)
    yield DataTransformer(ddf1, ddf2)


@pytest.fixture
def mock_csv(mocker):
    mocked_csv_data = mocker.mock_open(read_data="id,name,email\n"
    "1,John,john@mail.com\n"
    "2,Steve,steve@mail.com\n"
    "3,Alice,alice@mail.com\n")
    mocker.patch("builtins.open", mocked_csv_data)


def test_csv(mocker, importer):

    test_file_path = "/Lets/Import/This/file.csv"
    mocker.patch.object(importer, 'import_csv') 
    importer.import_file(test_file_path)
    
    importer.import_csv.assert_called_with(test_file_path)


def test_parquet(mocker, importer):
    test_file_path = "/Lets/Import/This/file.parquet"
    mocker.patch.object(importer, 'import_parquet')
    importer.import_file(test_file_path)
    
    importer.import_parquet.assert_called_with(test_file_path)


def test_no_extension(importer):
    test_file_path = "/Lets/Import/This/file"
    with pytest.raises(UnsupportedFileTypeException):
        importer.import_file(test_file_path)


def test_unsupported_filetype(importer):
    test_file_path = "/Lets/Import/This/file.xml"
    with pytest.raises(UnsupportedFileTypeException):
        importer.import_file(test_file_path)


def test_append_filename(transformer):

    file1_name = "/This/Is/The/FirstFile.csv"
    file2_name = "/This/Is/The/SecondFile.parquet"
    transformer.append_filenames_to_columns(file1_name, file2_name)

    assert transformer.left_suffix == "_FirstFile"
    assert transformer.right_suffix == "_SecondFile"

def test_json_report(transformer):

    expected_json = """{"Name":"John","Id":"1","Email":"john@mail.com"}
{"Name":"Steve","Id":"2","Email":"steve@mail.com"}
{"Name":"Alice","Id":"3","Email":"alice@mail.com"}"""

    join_columns = "Id"
    file1_columns = ["Name", "Id"]
    file2_columns = ["Email"]
    transformer.generate_json(join_columns, file1_columns, file2_columns)

    with open("report_0.json") as f:
        result_json = f.read()
        
    assert expected_json == result_json.strip()

    if os.path.exists("report_0.json"):
        os.remove("report_0.json")
