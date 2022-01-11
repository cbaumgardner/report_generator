"""

TODO: Come back and get this working in the container

import subprocess
import pytest
import filecmp

def test_integration():


    subprocess.run("python3 report_generator -file1 /Users/cameron/Documents/GitHub/report_generator/tests/integration/fixtures/students.csv -file1_cols fname lname cid -file2 /Users/cameron/Documents/GitHub/report_generator/tests/integration/fixtures/teachers.parquet -file2_cols fname lname -join_cols cid", shell=True, cwd="..")

    generated = "/Users/cameron/Documents/Github/report_0.json"
    control = "/Users/cameron/Documents/Github/report_generator/tests/integration/fixtures/report_0_expected.json"


    result = filecmp.cmp(generated, control)
    assert result

"""