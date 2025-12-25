

"""
PySpark ETL script for ingesting, harmonizing, and merging corporate data into an Apache Iceberg table.
Includes entity resolution across two sources.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from etl.entity_resolution import add_entity_resolution_columns

ICEBERG_CATALOG = "local_catalog"
ICEBERG_WAREHOUSE = "./iceberg_warehouse"
ICEBERG_DB = "corp_db"
ICEBERG_TABLE = "corporate_registry"
SOURCE1_PATH = "sample_data/source1_supply_chain.csv"
SOURCE2_PATH = "sample_data/source2_financial.csv"

# Define schemas for both sources
def get_source1_schema():
    return StructType([
        StructField("corporate_name_S1", StringType(), True),
        StructField("address", StringType(), True),
        StructField("activity_places", StringType(), True),
        StructField("top_suppliers", StringType(), True),
    ])

def get_source2_schema():
    return StructType([
        StructField("corporate_name_S2", StringType(), True),
        StructField("main_customers", StringType(), True),
        StructField("revenue", FloatType(), True),
        StructField("profit", FloatType(), True),
        StructField("address", StringType(), True),
    ])

def create_spark():
    return SparkSession.builder \
        .appName("IcebergETL") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE) \
        .getOrCreate()

def harmonize_sources(df1, df2):
    df1 = add_entity_resolution_columns(df1, "corporate_name_S1", "address", id_col="corp_id")
    df2 = add_entity_resolution_columns(df2, "corporate_name_S2", "address", id_col="corp_id")
    df1 = df1.withColumnRenamed("corporate_name_S1", "corporate_name_s1") \
             .withColumnRenamed("address", "address_s1") \
             .withColumn("source", lit("supply_chain"))
    df2 = df2.withColumnRenamed("corporate_name_S2", "corporate_name_s2") \
             .withColumnRenamed("address", "address_s2") \
             .withColumn("source", lit("financial"))
    df_merged = df1.join(df2, on="corp_id", how="outer")
    from pyspark.sql.functions import coalesce
    return df_merged.select(
        col("corp_id"),
        coalesce(col("corporate_name_s1"), col("corporate_name_s2")).alias("corporate_name"),
        coalesce(col("address_s1"), col("address_s2")).alias("address"),
        col("activity_places"),
        col("top_suppliers"),
        col("main_customers"),
        col("revenue"),
        col("profit")
    )

def main():
    spark = create_spark()
    df1 = spark.read.csv(SOURCE1_PATH, header=True, schema=get_source1_schema())
    df2 = spark.read.csv(SOURCE2_PATH, header=True, schema=get_source2_schema())
    harmonized_df = harmonize_sources(df1, df2)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DB}")
    spark.sql(f'''
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DB}.{ICEBERG_TABLE} (
            corp_id STRING,
            corporate_name STRING,
            address STRING,
            activity_places STRING,
            top_suppliers STRING,
            main_customers STRING,
            revenue FLOAT,
            profit FLOAT
        )
        PARTITIONED BY (corp_id)
    ''')
    harmonized_df.createOrReplaceTempView("staging")
    merge_sql = f'''
        MERGE INTO {ICEBERG_CATALOG}.{ICEBERG_DB}.{ICEBERG_TABLE} t
        USING staging s
        ON t.corp_id = s.corp_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    '''
    spark.sql(merge_sql)
    print("Harmonized data merged into Iceberg table.")
    spark.stop()

if __name__ == "__main__":
    main()
