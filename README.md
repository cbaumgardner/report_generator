# Report Generator


## About

CLI application that takes two files, the columns to join on, and the columns from each file
that you want in the report and generates a JSON document. 


## Some Things To Be Aware Of

* This application is currently configured to support reading .csv and .parquet files. 
* If the files have overlapping columns, the application will extract the file name (excluding extension) and append to the column name to differentiate.
* Instead of pandas, dask dataframes are used in case the files are too large to be kept in memory.

## Arguments
<pre>
  -file1          First file to read.  
  -file1_cols     Columns to select from file 1.  
  -file2          Second file to read.  
  -file2_cols     Columns to select from file 2.  
  -join_cols      Columns to join file 1 and file 2 on. Column name must be the same in each  
                  file.  
</pre>
## Example

This assumes that you already have Docker installed.

1) Place the input files in this directory
2) Build the image

  ```docker build -t report_generator_image --rm .```
  
3) Start the container

  ```docker run -it --name report_generator --rm --mount type=bind,source="$(pwd)"/,target=/home/app_user report_generator_image```
  
4) Run the report_generator command using the arguments explained above, or by calling ```python report_generator -h```

  ```python report_generator -file1 students.csv -file1_cols fname lname cid -file2 teachers.parquet -file2_cols fname lname -join_cols cid```
  
5) You can now enter ```exit``` to close the container and the json document(s) will be saved here.

