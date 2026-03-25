# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col

# COMMAND ----------

try:
    dbutils.widgets.text("env", "dev", "Environment (dev or prod)")
    env = dbutils.widgets.get("env")
except:
    env = "dev" # Fallback

# COMMAND ----------

CATALOG = f"indicium_{env}"
LANDING_VOLUME = f"/Volumes/{CATALOG}/bronze/landing_erp/"
SCHEMA_BRONZE = f"{CATALOG}.bronze"

# COMMAND ----------

tables_to_ingest = [
    "categories", "customers", "employee_territories", "employees", 
    "order_details", "orders", "products", "region", 
    "shippers", "suppliers", "territories", "us_states",
    "customer_customer_demo", "customer_demographics"
]

# COMMAND ----------


def ingest_table_to_bronze_uc(table_name):
    print(f"[{env.upper()}] Ingerindo: {table_name}...")
    source_file = f"{LANDING_VOLUME}{table_name}.csv"
    uc_table_name = f"{SCHEMA_BRONZE}.{table_name}"
    
    try:
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("sep", ";") \
            .option("inferSchema", "true") \
            .load(source_file)
        
        df_bronze = df \
            .withColumn("_source_file", col("_metadata.file_path")) \
            .withColumn("_insert_date", current_timestamp()) \
            .withColumn("_system_source", lit("ERP_POSTGRESQL"))
            
        df_bronze.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(uc_table_name)
            
        print(f"Sucesso: Tabela registrada como {uc_table_name}")
        
    except Exception as e:
        print(f"Erro na tabela {table_name}: {str(e)}")


for table in tables_to_ingest:
    ingest_table_to_bronze_uc(table)

# COMMAND ----------

for table in tables_to_ingest:
    ingest_table_to_bronze_uc(table)