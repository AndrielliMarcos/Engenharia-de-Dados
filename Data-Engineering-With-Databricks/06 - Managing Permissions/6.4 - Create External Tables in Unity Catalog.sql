-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Criar tabelas externas no Unity Catalog
-- MAGIC ###Objetivos:
-- MAGIC - Criar uma storage credential e uma external location
-- MAGIC - Controlar o acesso para a external lovation
-- MAGIC - Criar uma external table usando a external location
-- MAGIC - Gerenciar o controle de acessp aos arquivos usando a external locations

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Configurar
-- MAGIC Execute a célula abaixo para realizar algumas configurações. Para evitar conflitos em um ambiente de treinamento compartilhado, isso criará um banco de dados com nome exclusivo para o seu. Além disso, ele preencherá esse banco de dados com uma tabela de origem a ser usada posteriormente no exercício.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observação: este notebook assume um catálogo denomidado *main* em seu metastore do Unity Catalog. Se você precisar direcionar para um catálogo diferente, edite o notebook em **Classroom-Setup**.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-06.4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criar uma Storage Credential
-- MAGIC O primeiro pré-requisito para criar *external tables* é estabelecer uma credencial para acessar o armazenamento em nuvem onde os dados da tabela ficarão. No Unity Catalog, essa construção é referida a uma *storage credential* (credencial de armazenamento) e criaremos uma agora.
-- MAGIC 
-- MAGIC Precisamos de alguma informações para criar uma *storage credential*:
-- MAGIC - Para o Azure, exigimos um diretório, uma *application ID* e um *client secret* de um *service principal* que recebeu a função de **Azure Blob Contributor** no local de armazenamento. Consulte <a href="https://docs.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-tables#create-an-external-table" target="_blank">este documento</a> pata obter mais informaçãoes.
-- MAGIC 
-- MAGIC Com essa informações necessárias, vamos para a persona SQL e, continuando a seguir o documento relevante vinculado acima, vamos criar uma *storage credential*, anote o nome que você atribui e retorne a este notebook.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Para ver as propriedades da *storage credential* use o SQL da seguinte maneira. Descomente a célula a seguir e substitua o nome que você atribuiu à sua *storage credential*.

-- COMMAND ----------

-- DESCRIBE STORAGE CREDENTIAL <storage credential name>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Embora você possa usar *storage credential* para gerenciar o acesso a dados externos, o Databricks recomenda usar **external locations**, que contêm uma definição de path completa que faz a referência à *storage credential* apropriada. O uso de *external locations* em sua estratégia de governança em vez de *storage credential* fornece um grau mais refinado de controle sobre seu armazenamento em nuvem. Vamos criar uma a seguir.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criar uma External Location
-- MAGIC Embora possamos usar *storage locations* diretamente para gerenciar o acesso aos nossos recursos externos, o Unity Catalog também fornece uma **external location** que especifica adicionalmente uma path dentro do conteinêr de armazenamento. O uso de *external locations* para controle de acesso é a abordagem preferida, pois nos dá controle no nível da path do arquivo, e não no nível do próprio contêiner de armazenamento.
-- MAGIC 
-- MAGIC Para definir uma *estrenal location*, precisamos de algumas informações adicionais:
-- MAGIC - Para o Azure, exigimos a path do contêiner de armazenamento. Consulte <a href="https://docs.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-tables#create-an-external-location" target="_blank">este documento</a> para obter mais detalhes.
-- MAGIC 
-- MAGIC Vamos voltar para a persona SQL e, continuando a seguir o documento acima, vamos criar uma *external location*. Se desejar, você pode atribuir sua *external location* com o mesmo nome da *storage credential*, embora isso geralmente não seja uma boa prática, pois geralmente haverá muitas *external locations* referenciando uma *storage credential*. De qualquer forma, anote o nome que você atribuiu e volte para este notebook.
-- MAGIC 
-- MAGIC Seguindo o guis apropriado com base em seu provedor de nuvem, crie a *external location* na página **Data** da persona **SQL** e retorne para este notebook.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Para ver as propriedades da *external location*, use o SQL da seguinte maneira. Descomente a célula a seguir e substitua pelo nome que você atribuiu a sua *external location*.

-- COMMAND ----------

-- DESCRIBE EXTERNAL LOCATION <external location name>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe o valor **url** da saída da célula anterior. Vamos precisar disso para criar nossa tabela externa a seguir.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criar uma external table
-- MAGIC Com uma *external location* definida, vamos usá-la para criar uma *external table*. Para este exemplo, usaremos a instrução **CREATE TABLE AS SELECT** para fazer uma cópia da tabela **silver_managed** que foi criada como parte da configuração.
-- MAGIC 
-- MAGIC A sintaxe para criar uma *exteranl table* é idêntica a uma tabela gerenciada, mas com a adição de uma especificação **LOCATION** que especifica a path de armazenamento em nuvem completo contendo os arquivos de dados da tabela.
-- MAGIC 
-- MAGIC Como não estamos usando *storage credentials* para gerenciar o acesso ao nosso armazenamento, essa operação só poderá ser bem-sucedoda se:
-- MAGIC - somos proprietários de um objeto *external location* que cobre a localização especificada (o que é o caso aqui), ou
-- MAGIC - temos o privilégio **CREATE_TABLE** no objeto *external location* que cobre a localização especificada.
-- MAGIC 
-- MAGIC E, assim comonas tabelas gerenciadas, é claro que também devemos ter o privilégio **CREATE** no banco de dados, bem como **USAGE** no banco de dados e no catálogo.
-- MAGIC 
-- MAGIC Descomente a célula abaixo e substitua o valor de **url** acima.

-- COMMAND ----------

-- CREATE OR REPLACE TABLE silver_external
-- LOCATION '<url value from above>'
-- AS SELECT * FROM silver_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Conceder acesso uma uma external table [opcional]
-- MAGIC Uma vez que a external table é criada, o controle de acesso trabalha da mesma forma como na tabela gerenciada.
-- MAGIC 
-- MAGIC Observe que você só pode executar esta seção se tiver seguido o exercício *Gerenciar usuários e grupos* e criado um grupo no Unity Catalog chamado *analysts*.
-- MAGIC 
-- MAGIC Execute esta seção descomentando as céluas de código e executando-as em sequência. Você també será solicitado a executar algumas consultas como um usuário secundário. 
-- MAGIC Para fazer isso:
-- MAGIC 1. Abra uma sessão de navegação privada separada e faça login no Databricks SQL usando a ID de usuário que você criou ao executar *Gerenciar usuários e grupos* 
-- MAGIC 1. Crie um terminal SQL seguindo as instruções em *Criar terminal SQL no Unity Catalog*
-- MAGIC 1. Prepare-se para inserir consultas conforme as instruções abaixo nesse ambiente
-- MAGIC 
-- MAGIC Vamos executar a célula a seguir para gerar uma instrução de consulta que lê a *external table*. Copie e cole a saída em uma nova consulta no ambiente Databricks SQL do seu usuário secundário e excute a consulta.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"SELECT * FROM {DA.catalog_name}.{DA.schema_name}.silver_external")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Naturalmente isso irá falhar sem as grants apropriadas:
-- MAGIC * **USAGE** no catálogo
-- MAGIC * **USAGE** no database
-- MAGIC * **SELECT** na tabela
-- MAGIC 
-- MAGIC Vamos abordar isso agora com as três declarações **GRANT** a seguir. Descomente e execute a célula a seguir.

-- COMMAND ----------

-- GRANT USAGE ON CATALOG `${da.catalog_name}` to `analysts`;
-- GRANT USAGE ON DATABASE `${da.schema_name}` TO `analysts`;
-- GRANT SELECT ON TABLE silver_external to `analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Reexecute a consulta como seu usuário. Como esperado, agora terá sucesso.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Comparando tabelas externas e gerenciadas
-- MAGIC Vamos fazer uma comparação de alto nível das duas tabelas usando **DESCRIBE EXTENDED**.

-- COMMAND ----------

-- DESCRIBE EXTENDED silver_managed

-- COMMAND ----------

-- DESCRIBE EXTENDED silver_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Na maioria das vezes, a tabelas são semelhantes, mas a principal diferença está na **LOCATION**.
-- MAGIC 
-- MAGIC Agora vamos comparar o comportamento quando eliminamos e recriamos essas tabelas. Antes de fazermos isso, vamos usar **SHOW CREATE TABLE** para capturar os comando necessários para recriar essa tabelas depois de eliminá-las. Descomente e execute as células a seguir.

-- COMMAND ----------

-- SHOW CREATE TABLE silver_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora vamos descartar as duas tabelas.

-- COMMAND ----------

-- DROP TABLE silver_managed
-- DROP TABLE silver_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora, vamos copiar e colar a expressão **CREATE TABLE** acima para recriar a tabela **silver_managed**.

-- COMMAND ----------

-- paste createtab_stmt for silver_managed from above

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Descomente e execute a célula a seguir para exibir o conteúdo da tabela **silver_managed** recriada.

-- COMMAND ----------

-- SELECT * FROM silver_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Vamos recriar a tabela **silver_external**, especificando o mesmo schema de **silver_managed** mas com mesmo **LOCATION** que usamos anteriormente para criar esta tabela externa inicialmente, então exibir o conteúdo.

-- COMMAND ----------

-- CREATE TABLE silver_external (
--   device_id INT,
--   mrn STRING,
--   name STRING,
--   time TIMESTAMP,
--   heartrate DOUBLE
-- ) 
-- LOCATION '<url value from earlier>';

-- SELECT * FROM silver_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe esta diferença fundamental no comportamento. No caso gerenciado, todos os dados da tabela foram descartados quando a tabela foi eliminada. No caso externo, como o Unity Catalog  não gerencia os dados, ele foi retido recriando a tabela usando o mesmo local.
-- MAGIC 
-- MAGIC Antes de prosseguir para a proxima seção, vamos descomentar e executar a célula a seguir para executar um pouco de limpeza descartando a tabela **silver_external**. Observe novamente que isso apenas eliminará a tabela. Os arquivos de dados que suportam a tabela serão retidos.

-- COMMAND ----------

-- DROP TABLE silver_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Conceder acesso aos arquivos
-- MAGIC Anteriormente, criamos uma *storage credential* e uma *external location* para nos permitir criar uma *external table* cujos arquivos de dados residem em um armazenamento em nuvem externo.
-- MAGIC 
-- MAGIC *Storage credential* e *external location* oferecem suporte a privilégios espevializados que nos permitem controlar o acesso aos arquivos armazenados nesses lovais. Esses privilégios incluem:
-- MAGIC * **READ FILES**: capacidade de ler diretamente os arquivos armazenados neste local
-- MAGIC * **WRITE FILES**: capacidade de gravar diretamente arquivos armazenados neste local
-- MAGIC * **CREATE TABLE**: capacidade de criar uma tabela com base nos arquivos armazenados neste local
-- MAGIC 
-- MAGIC A instrução SQL a seguir nos mostra uma lista dos arquivos no local especificado. Descomente e execute a célula a seguir, volocando o valor **url** usado anteriormente para criar a external table **silver_external**.

-- COMMAND ----------

-- LIST '<url value from earlier>';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora vamos executar um **SELECT** na tabela representada por esses arquivos de dados. Lembre-se de que a tabela que esse arquivos de dados estavam foi elimidada. Agora estamos simplesmente executabdo esta operação em arquivos de dados brutos que costumavam fazer backup da tabela **silver_external**.

-- COMMAND ----------

-- SELECT * FROM delta.`<same url value as previous command>`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Nenhum privilégio adicional é necessário para fazer essa últimas operações, pois somos os proprietários do objeto da *external location* que cobre esse caminho.
-- MAGIC 
-- MAGIC Se você estiver acompanhando como um usuário secundário no Databricks SQL, copie e cole o código acima como uma nova consulta nesse ambiente e execute-o.
-- MAGIC 
-- MAGIC Isso falha porque o usuário não tem GRANT **READ FILES** no local. Vamos corrigir isso agora executando o seguinte comando, substituindo o nome da *external location* criado anteriormente.

-- COMMAND ----------

-- GRANT READ FILES ON EXTERNAL LOCATION <external location name> TO `analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora tente a operação **SELECT** novamente no ambiente Databricks SQL. Exercendo a GRANT **READ FILES**, agora você terá sucesso.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
