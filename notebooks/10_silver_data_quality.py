# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------


catalog_name = "indicium_dev" 
schema_name = "silver" 
full_schema_path = f"{catalog_name}.{schema_name}"

dq_logs = []

tables = spark.catalog.listTables(full_schema_path)

for table in tables:
    table_name = table.name
    full_table_name = f"{full_schema_path}.{table_name}"
    
    print(f"Analisando tabela: {full_table_name}...")
    
    try:
  
        df = spark.table(full_table_name)
        total_rows = df.count()
        
        if total_rows == 0:
            dq_logs.append((table_name, "ALL", "Tabela Vazia", 0, 0))
            continue
            
        for col_name, col_type in df.dtypes:
            
            null_count = df.filter(F.col(col_name).isNull()).count()
            if null_count > 0:
                dq_logs.append((table_name, col_name, "Valores Nulos", null_count, total_rows))
                
            if col_type == 'string':
                empty_count = df.filter((F.col(col_name) == "") | (F.trim(F.col(col_name)) == "")).count()
                if empty_count > 0:
                    dq_logs.append((table_name, col_name, "Strings Vazias", empty_count, total_rows))
                    
            if col_type in ['int', 'bigint', 'double', 'float', 'decimal']:
                negative_count = df.filter(F.col(col_name) < 0).count()
                if negative_count > 0:
                    dq_logs.append((table_name, col_name, "Valores Negativos", negative_count, total_rows))
                    
    except Exception as e:
        print(f"Erro na tabela {table_name}: {e}")
        dq_logs.append((table_name, "ERRO", str(e), 0, 0))

log_schema = ["table_name", "column_name", "issue_type", "issue_count", "total_rows"]

if dq_logs:
    df_log = spark.createDataFrame(dq_logs, schema=log_schema)
    df_log = df_log.withColumn(
        "issue_percentage", 
        F.round(
            F.when(F.col("total_rows") != 0, F.col("issue_count") / F.col("total_rows"))
            .otherwise(0) * 100, 2
        )
    ).withColumn(
        "execution_timestamp", 
        F.current_timestamp()
    )
    display(df_log)
else:
    print("Nenhum problema encontrado")

# COMMAND ----------


audit_catalog = catalog_name 
audit_schema = "audit"         
log_table_name = "dq_execution_logs"
full_log_table_path = f"{audit_catalog}.{audit_schema}.{log_table_name}"
volume_path = f"/Volumes/{audit_catalog}/{audit_schema}/log_files/dq_log_{F.current_date().expr}.csv"

if dq_logs:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {audit_catalog}.{audit_schema}")
    (df_log.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(full_log_table_path))
    print(f"Histórico de auditoria atualizado com sucesso na tabela {full_log_table_path}.")
    try:
        df_log_pandas = df_log.toPandas()
        df_log_pandas.to_csv(volume_path, index=False, encoding='utf-8')
        print(f"log guardado em: {volume_path}")
    except Exception as e:
        print(f"Aviso: Não foi possível gravar o ficheiro no Volume. Erro: {e}")
else:
    print("Nenhum problema de qualidade encontrado!")
