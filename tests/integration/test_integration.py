import subprocess
import pytest
import filecmp

def test_integration():

    """
    Ok, this is EXTREMELY hacky and wouldn't work on 
    another machine without changing the hardcoded paths which
    obviously isn't ideal, but this is what I got to work
    before running out of time.
    """

    subprocess.run("python3 report_generator -file1 /Users/cameron/Documents/GitHub/report_generator/tests/integration/fixtures/students.csv -file1_cols fname lname cid -file2 /Users/cameron/Documents/GitHub/report_generator/tests/integration/fixtures/teachers.parquet -file2_cols fname lname -join_cols cid", shell=True, cwd="..")

    generated = "/Users/cameron/Documents/Github/report_0.json"
    control = "/Users/cameron/Documents/Github/report_generator/tests/integration/fixtures/report_0_expected.json"


    result = filecmp.cmp(generated, control)
    assert result

