import pytest

from CommonUtilities.Utilities import check_table_no_duplicate_keys, mysql_conn_source, check_column_not_null, \
    check_row_count_match, check_referential_integrity, mysql_con_target
from Configuration.Config import COLUMN_NAME

# -------------------------------
# SQL-LEVEL CHECKS
# -------------------------------
@pytest.mark.smoke

def test_table_has_no_duplicate_keys():
    query = "SELECT * FROM stagedb.stage_sales"
    assert not check_table_no_duplicate_keys(query, ["sales_id"], mysql_conn_source)
@pytest.mark.smoke

def test_column_sales_id_not_null():
    query = "SELECT sales_id FROM targetdb.fact_sales"
    assert not check_column_not_null(query, "sales_id", mysql_con_target)

@pytest.mark.smoke

def test_row_count_matches():
    source_query = "SELECT count(*) FROM stagedb.stage_sales"
    target_query = "SELECT count(*) FROM targetdb.fact_sales"
    assert not check_row_count_match(source_query, target_query, mysql_conn_source ,mysql_con_target)
@pytest.mark.smoke

def test_referential_integrity_sales_id():
    source_query = "SELECT DISTINCT sales_id FROM stagedb.stage_sales"
    target_query = "SELECT DISTINCT sales_id FROM targetdb.fact_sales"
    assert not check_referential_integrity(source_query, target_query, COLUMN_NAME, mysql_conn_source ,mysql_con_target)