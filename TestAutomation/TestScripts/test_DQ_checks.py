import os

from CommonUtilities.Utilities import (
    check_file_exists,
    check_file_size,
    check_file_no_duplicates,
    check_file_duplicate_column,
    check_file_null_values,
    check_table_no_duplicate_keys,
    mysql_conn_source,
    check_column_not_null, mysql_con_target,
    check_row_count_match,
    check_referential_integrity,


)
from Configuration.Config import COLUMN_NAME

# Ensure LogFile exists
os.makedirs("LogFile", exist_ok=True)


# -------------------------------
# FILE-LEVEL CHECKS
# -------------------------------
def test_file_exists():
    assert check_file_exists("TestData/sales_data.csv"), "File missing"

def test_check_file_size():
    assert check_file_size("TestData/sales_data.csv"), "File Empty"


def test_no_duplicate_rows():
    assert not check_file_no_duplicates("TestData/sales_data.csv", "csv"), "Duplicate rows found"


def test_no_duplicate_column_sales_id():
    assert not check_file_duplicate_column("TestData/sales_data.csv", "csv", "sales_id"), \
        "Duplicate sales_id found"


def test_no_null_values():
    assert not check_file_null_values("TestData/sales_data.csv", "csv"), "NULL values found"
