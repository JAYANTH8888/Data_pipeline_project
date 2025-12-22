

import pytest
from pyspark.sql import SparkSession
from etl.entity_resolution import clean_corporate_name, normalize_address, generate_corp_id, add_entity_resolution_columns


def spark_session():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()




def test_clean_corporate_name():
    assert clean_corporate_name("Acme Corp.") == "acme"
    assert clean_corporate_name("Beta Incorporated") == "beta incorporated"
    assert clean_corporate_name("Gamma LLC") == "gamma"

def test_normalize_address():
    assert normalize_address("123 Main St.") == "123 main st"
    assert normalize_address("456 Elm St, Suite 5") == "456 elm st suite 5"

def test_generate_corp_id():
    name = "Acme Corp."
    address = "123 Main St."
    corp_id = generate_corp_id(name, address)
    assert corp_id.startswith("acme_123 main st")

def test_add_entity_resolution_columns():
    spark = spark_session()
    data = [("Acme Corp", "123 Main St")]
    df = spark.createDataFrame(data, ["corporate_name", "address"])
    df2 = add_entity_resolution_columns(df, "corporate_name", "address")
    row = df2.collect()[0]
    assert row.clean_name == "acme"
    assert row.norm_address == "123 main st"
    assert row.corp_id.startswith("acme_123 main st")
    spark.stop()
