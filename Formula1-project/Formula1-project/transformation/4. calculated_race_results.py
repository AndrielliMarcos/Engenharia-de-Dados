# Databricks notebook source
# MAGIC %md
# MAGIC #### Pilotos e equipes dominantes

# COMMAND ----------

# DBTITLE 1,Definir a data do arquivo
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,Criar tabela
spark.sql(f"""
          CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
          (
            race_year INT,
            team_name STRING,
            driver_id INT,
            driver_name STRING,
            race_id INT,
            position INT,
            points INT,
            calculated_points INT,
            created_date TIMESTAMP,
            updated_date TIMESTAMP
          )
          USING DELTA
          """)

# COMMAND ----------

# DBTITLE 1,Criar Temp View para fazer o MERGE
spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW race_result_updated
            AS
            SELECT races.race_year,
                    constructors.name AS team_name,
                    drivers.driver_id,
                    drivers.name AS driver_name,
                    races.race_id,
                    results.position,
                    results.points,
                    11 - results.position AS calculated_points
            FROM f1_processed.results
            JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
            JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
            JOIN f1_processed.races ON (results.race_id = races.race_id)
            WHERE results.position <= 10
                AND results.file_date = '{v_file_date}'
          """)

# COMMAND ----------

# DBTITLE 1,MERGE - inserir/atualizar os dados
spark.sql(f"""
          MERGE INTO f1_presentation.calculated_race_results tgt
            USING race_result_updated upd 
            ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
            WHEN MATCHED THEN
            UPDATE SET tgt.position = upd.position,
                        tgt.points = upd.points,
                        tgt.calculated_points = upd.calculated_points,
                        tgt.updated_date = current_timestamp
            WHEN NOT MATCHED THEN
            INSERT (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, created_date) VALUES (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, current_timestamp)
          """)


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from race_result_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from f1_presentation.calculated_race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Algumas anomalias foram encontradas ao analisar os pontos dos pilotos que terminavam em primeiro lugar. Por exemplo, em 1954, o piloto que terminou a corrida em primeiro lugar teve 8 pontos, já em 2017 o piloto que terminou a corrida em primeiro lugar teve 25 pontos. Com isso, não é possível somar os pontos de um piloto, ou equipe, e dizer que este foi o dominante de todos os tempos. O correto para fazer esta análise seria em todos os anos os pilotos receberem a mesma quantidade de pontos. Para resolver este probelema, iremos padronizar os pontos usando a posição do piloto. O piloto que terminar em primeiro lugar (posição = 1) receberá 10 pontos (11 - results.position AS calculated_points). O segundo lugar receberá 9 pontos, assim por diante até o 10º lugar (WHERE results.position <= 10). 