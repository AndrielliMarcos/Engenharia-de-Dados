# Databricks notebook source
# MAGIC %md
# MAGIC #Explorando os logs de evento do pipeline
# MAGIC O DLT usa os logs de eventos para armazenar muitas das informações importantes usadas para gerenciar, relatar e entender o que está acontecendo durante a execução do pipeline.
# MAGIC 
# MAGIC Abaixo, forneceomos várias consultas úteis para explorar o log de eventos e obter mais informações sobre seus pipelines.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-04.4

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log de eventos de consulta
# MAGIC O log de eventos é gerenciado como uma tabela Delta Lake com alguns dos campos mais importantes armazenados como dados JSON aninhados.
# MAGIC 
# MAGIC A consulta abaixo mostra como é simples ler esta tabela e criar um DataFrame e uma view temporária para consulta interativa.

# COMMAND ----------

event_log_path = f"{DA.paths.storage_location}/system/events"

event_log = spark.read.format('delta').load(event_log_path)
event_log.createOrReplaceTempView("event_log_raw")

display(event_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Definir ID da atualização mais recente
# MAGIC Em muitos casos, você pode querer obter atualizações sobre a última atualização (ou as últimas N atualizações) em seu pipeline.
# MAGIC 
# MAGIC Podemos capturar facilmente o ID de atualização mais recente comuma consulta SQL.

# COMMAND ----------

latest_update_id = spark.sql("""
    SELECT origin.update_id
    FROM event_log_raw
    WHERE event_type = 'create_update'
    ORDER BY timestamp DESC LIMIT 1""").first().update_id

print(f"Latest Update ID: {latest_update_id}")

# Push back into the spark config so that we can use it in a later query.
spark.conf.set('latest_update.id', latest_update_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Executar registro de autoria
# MAGIC Os eventos relacionados à execução de pipeline e à edição de configuração são capturados como **`user_action`**.
# MAGIC 
# MAGIC O seu deve deve ser o único **`user_name`** para o pipeline que você configurou durante esta lição.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT timestamp, details:user_action:action, details:user_action:user_name
# MAGIC FROM event_log_raw 
# MAGIC WHERE event_type = 'user_action'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Examinar Lineage
# MAGIC A DLT fornece informações de linhagem integradas sobre como os dados flues pela sua tabela.
# MAGIC 
# MAGIC Embora a consulta abaixo indique apenas os predecessores diretos para cada tabela, essa informações podem ser facilmente combinadas para ratrear dados em qualquer tabela de volta ao pono em que entraram no lakehouse.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT details:flow_definition.output_dataset, details:flow_definition.input_datasets 
# MAGIC FROM event_log_raw 
# MAGIC WHERE event_type = 'flow_definition' AND 
# MAGIC       origin.update_id = '${latest_update.id}'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Examinar métricas de qualidade dos dados
# MAGIC Por fim, as métricas de qualidade de dados podem ser extremamente úteis para insights de longo e curto prazo sobre seus dados.
# MAGIC 
# MAGIC Abaixo, capturamos as métricas para cada restrição ao longo de todo o tempo de vida de nossa tabela.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT row_expectations.dataset as dataset,
# MAGIC        row_expectations.name as expectation,
# MAGIC        SUM(row_expectations.passed_records) as passing_records,
# MAGIC        SUM(row_expectations.failed_records) as failing_records
# MAGIC FROM
# MAGIC   (SELECT explode(
# MAGIC             from_json(details :flow_progress :data_quality :expectations,
# MAGIC                       "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>")
# MAGIC           ) row_expectations
# MAGIC    FROM event_log_raw
# MAGIC    WHERE event_type = 'flow_progress' AND 
# MAGIC          origin.update_id = '${latest_update.id}'
# MAGIC   )
# MAGIC GROUP BY row_expectations.dataset, row_expectations.name
