# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

def add_ingestion_date(input_df):
    """
    função que recebe um DataFrame como parâmetro e retorna uma nova coluna para este Dataframe.
    """
    return input_df.withColumn('ingestion_date', current_timestamp())


# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
    
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition) \
          .whenMatchedUpdateAll() \
          .whenNotMatchedInsertAll() \
          .execute()
    else:
       input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    """
    função que recebe um dataframe e reorganiza suas colunas de acirdo com que a coluna que será usada como partição seja a última do dataframe
    """
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

# criar uma lista vazia e popular com os anos das corridas
def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list