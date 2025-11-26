import os
from CommonUtilities.Utilities import check_for_duplicate_rows_in_file, check_sales_data_duplicate_column, \
    check_for_null_value_in_file, check_file_exists, check_file_size

os.makedirs("LogFile", exist_ok=True)

def test_sales_data_duplicate_check():
    has_duplicates = check_for_duplicate_rows_in_file("TestData/sales_data.csv", "csv")
    assert not has_duplicates, "There are duplicate rows in file"


def test_sales_data_duplicate_col_check():
    has_duplicates_col = check_sales_data_duplicate_column("TestData/sales_data.csv", "csv",
                                                                         "sales_id")
    assert not has_duplicates_col, "There are duplicate in column in file"

def test_null_value_check():
    has_null = check_for_null_value_in_file("TestData/sales_data.csv" ,"csv")
    assert  not has_null, "There are nuls in file"

def test_sales_file_availability_check():
    does_file_exist = check_file_exists("TestData/sales_data.csv")
    assert does_file_exist, "File does not exist"

def test_sales_file_size_check():
    file_size = check_file_size("TestData/sales_data.csv")
    assert file_size,"File does not have expected size"



