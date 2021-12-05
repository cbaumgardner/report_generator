# Report Generator


## About

CLI application that takes two files, the columns to join on, and the columns from each file
that you want in the report and generates a JSON document. 


## Some things to be aware of

* This application is currently configured to support csv and parquet files. 
* If the files have overlapping columns, the application with extract the file name (excluding extension) and append to the column name to differentiate.
* Instead of pandas, dask dataframes are used in case the files are too large to be kept in memory

## Arguments
  -file1          First file to read.
  -file1_cols     Columns to select from file 1.
  -file2          Second file to read.
  -file2_cols     Columns to select from file 2.
  -join_cols      Columns to join file 1 and file 2 on. Column name must be the same in each 				 file.

## Example call
```python3 report_generator -file1 /Path/to/file/students.csv -file1_cols fname lname cid -file2 /Path/to/file/teachers.parquet -file2_cols fname lname -join_cols cid``` 


