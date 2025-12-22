# jm_requirement
# Entity resolution utilities for corporate data harmonization
import re
from pyspark.sql.functions import udf, col, lower, regexp_replace, trim
from pyspark.sql.types import StringType

def clean_corporate_name(name):
    if not name:
        return ""
    name = name.lower()
    name = re.sub(r'[^a-z0-9 ]', '', name)
    name = re.sub(r'\b(inc|corp|ltd|llc|co|plc)\b', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

def normalize_address(address):
    if not address:
        return ""
    address = address.lower()
    address = re.sub(r'[^a-z0-9 ]', '', address)
    address = re.sub(r'\s+', ' ', address)
    return address.strip()

def generate_corp_id(name, address):
    # Simple heuristic: cleaned name + normalized address
    return f"{clean_corporate_name(name)}_{normalize_address(address)}"

def add_entity_resolution_columns(df, name_col, address_col, id_col="corp_id"):
    clean_name_udf = udf(clean_corporate_name, StringType())
    norm_addr_udf = udf(normalize_address, StringType())
    gen_id_udf = udf(generate_corp_id, StringType())
    return df.withColumn("clean_name", clean_name_udf(col(name_col))) \
             .withColumn("norm_address", norm_addr_udf(col(address_col))) \
             .withColumn(id_col, gen_id_udf(col(name_col), col(address_col)))
