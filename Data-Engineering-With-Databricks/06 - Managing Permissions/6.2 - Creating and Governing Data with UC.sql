-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Criando e governando objetos de dados com Unity Catalog
-- MAGIC
-- MAGIC ###Objetivos:
-- MAGIC - Criar catálogos, schemas, tables, views e udf(funções definidas por usuário)
-- MAGIC - Controlar o acesso desses objetos
-- MAGIC - Usar dynamic views para proteger colunas e linhas dentro das tabelas
-- MAGIC - Explorar as grants em vários objetos do Unity Catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Pré-Requisitos
-- MAGIC Para seguir neste laboratório é necessário:
-- MAGIC - Ter capacidade de metastore admin para criar e gerenciar um catálogo
-- MAGIC - Concluir os procedimentos descritos nos laboratórios do curso *Overview of Unity Catalog*:
-- MAGIC     - *Managing principals in Unity Catalog* (especificamente, você precisa de um grupo *analysts* contendo outro usuário com acesso ao Satabricks SQL)
-- MAGIC     - *Creating compute resources for Unity Catalog access* (especificamente, você precisa de um SQL warehouse ao qual o usuário mencionado acima tenha acesso)

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-06.2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Os três níveis de namespace do Unity Catalog
-- MAGIC Qualquer pessoa com experiência em SQL provavelmente estará familiarizada com o namespace tradicional de dois níveis para endereços de tabelas ou views em um schema da seguinte maneira:
-- MAGIC
-- MAGIC     SELECT * FROM myschema.mytable;
-- MAGIC     
-- MAGIC O Unity Catalog apresenta o conceito de **catalog** na hierarquia. Como um contêiner para schemas, o catálogo fornece uma nova forma para orgazinações segregar seus dados. Pode haver quantos catálogos você quiser, que por sua vez pode conter quantos schemas você quiser (o conceito de schema não é alterado pelo Unity Catalog. Os schemas contém objetos de dados como tabelasm views e UDFs)
-- MAGIC
-- MAGIC Para lidar com esse nível adicional, as referências completas tabela/view no Unity Catalog usam um namespace de três níveis:
-- MAGIC
-- MAGIC     SELECT * FROM mycatalog.myschema.mytable;
-- MAGIC     
-- MAGIC Isso pode ser útil em muitos casos de uso. Por exemplo:
-- MAGIC - Separação dos dados relativos a unidades de negócios dentro de sua organização (vendas, RH, marketing, etc)
-- MAGIC - Satisfazer os requisitos SDLC (dev, staging, prod, etc)
-- MAGIC - Estabelecendo sandboxes contendo conjuntos de dados temporários para uso interno

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criando um novo catálogo
-- MAGIC Vamos criar um novo catálogo em nosso metastore. A variável **`${DA.my_new_catalog}`** foi exibida pela célula de configuração acima, contendo uma string única gerada com base no seu nome de usuário.
-- MAGIC
-- MAGIC Execute a instrução **`CREATE`** a seguir e clique no ícone **Data** na barra lateral a esquerda para confirma que o novo catalogo foi criado.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS ${DA.my_new_catalog}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Selecionar um catálogo default
-- MAGIC Os desenvolvedores de SQL provavelmente também estarão familiarizados com a instrução **`USE`** para selecionar um schema padrão, encurtando assim as consultas por não precisar especificá-lo o tempo todo. Para estender essa conveniência ao lidar com o nível extra no namespace, o Unity Catalog aumenta a linguagem com duas instruções adicionais: 
-- MAGIC
-- MAGIC     USE CATALOG mycatalog;
-- MAGIC     USE SCHEMA myschema;  
-- MAGIC     
-- MAGIC Vamos selecionar o catálogo recém-criado como padrão. Agora, todas as referências de schema serão consideradas neste catálogo, a menos que sejam explicitamente substituídas por uma referência de catálogo.

-- COMMAND ----------

USE CATALOG ${DA.my_new_catalog}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criar e usar um novo schema
-- MAGIC Vamos criar um schema neste novo catálogo. Nós não precisaremos gerar outro nome exclusivo para este schema, pois agora estamos usando um catálogo exclusivo isolado do restante do metastore. Agora todas as referências de dados serão consideradas no catálogo e no schema que criamos, a menos que sejam explicitamente substituídas por uma referência de dois ou três níveis.
-- MAGIC
-- MAGIC Execute a célula abaixo e clique no ícone **Data** na barra lateral para confirmar que este schema foi criado no novo catálogo que criamos.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS example;
USE SCHEMA example

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Configurar tabelas e views
-- MAGIC Vamos configurar as tabelas e views. Para este exemplo, usaremos dados fictícios para criar e preencher uma tabela Silver Managed com dados sintéticos de frequência cardíaca do paciente e uma view Gold que calcula a média diária dos dados de frequência cardíaca por paciente.
-- MAGIC
-- MAGIC Excute a célula abaixo e clique no ícone **Data** na barra lateral esquerda para explorar o contepudo do schema *example*. Observe que não precisamos especificar os três níveis quando especificamos o nome da tabela ou view baixo, pois selecionamos um catálogo e um schema padrão.

-- COMMAND ----------

CREATE OR REPLACE TABLE silver (device_id INT, mrn STRING, name STRING, time TIMESTAMP, heartrate DOUBLE);

INSERT INTO silver VALUES
  (23,'40580129','Nicholas Spears','2020-02-01T00:01:58.000+0000',54.0122153343),
  (17,'52804177','Lynn Russell','2020-02-01T00:02:55.000+0000',92.5136468131),
  (37,'65300842','Samuel Hughes','2020-02-01T00:08:58.000+0000',52.1354807863),
  (23,'40580129','Nicholas Spears','2020-02-01T00:16:51.000+0000',54.6477014191),
  (17,'52804177','Lynn Russell','2020-02-01T00:18:08.000+0000',95.033344842);
  
SELECT * FROM silver

-- COMMAND ----------

CREATE OR REPLACE VIEW gold AS (
  SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
  FROM silver
  GROUP BY mrn, name, DATE_TRUNC("DD", time)
);
SELECT * FROM gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Consultar a tabela acima funciona como esperado, pois somos os proprietários dos dados. Ou seja, temos a propriedade do objeto de dados que estamos consultando. Consultar a view também funciona porque somos os proprietários da view e da tabela à qual ela faz referência. Portanto, nenhuma permissão em nível de objeto é necessária para acessar esses recursos.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Conceder acesso a objeto de dados (Grant)
-- MAGIC O Unity Catalog emprega um modelo de permissão explícita por padrão. Nenhuma permissão é implícita ou herdada. Portanto, para acessar quaiquer objetos de dados, os usuários precisarão da permissão **USAGE** em todos os elementos contidos, ou seja, o schema e o catálogo que o contém.
-- MAGIC
-- MAGIC Agora vamos permitir que os membros do grupo *analysts* consultem a view *gold*. Para fazer isso, precisamos conceder as seguintes permissões:
-- MAGIC 1. USAGE no catálogo e schema
-- MAGIC 2. SELECT no objeto de data (view no nosso exemplo)

-- COMMAND ----------

GRANT USAGE ON CATALOG ${DA.my_new_catalog} TO analysts;
GRANT USAGE ON SCHEMA example TO analysts;
GRANT SELECT ON VIEW gold to analysts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Consultar uma view como um analista
-- MAGIC Com uma hierarquia de objetos de dados e todas as permissões de acesso apropriadas em vigor, vamos tentar executar uma consulta na view gold como um usuário diferente. Lembre-se de que, na seção de pré-requisitos deste laboratório, fizemos refrência a um grupo chamado *analysts* contendo outro usuário.
-- MAGIC
-- MAGIC Nesta seção, executaremos consultas como esse usuário para verificar nossa configuração e observar o impacto quando fizemos alterações.
-- MAGIC
-- MAGIC Para preparar esta seção, **você irá precisar logar no Databricks usando uma sessão no browser separado.** Pode ser uma sessão privada, um perfil diferente se o seu navegador suportar perfis ou um navegador completamente diferente. Não basta abrir uma nova aba ou janela usando a mesma sessão do navegador, isso levará a conflitos de login.
-- MAGIC 1. Em um browser separado <a href="https://accounts.cloud.databricks.com/workspace-select" target="_blank">cole este link para fazer o login no Databricks</a> usando as suas credenciais de usuário
-- MAGIC 1. Alterne para a persona SQL
-- MAGIC 1. Vá para **Queries** e clique em **Create query**
-- MAGIC 1. Selecione o SQL warehouse compartilhado que foi criado enquanto seguia a aula *Criando rescursos de computação para acessar o Unity Catalog.*
-- MAGIC 1. Retorne para este notebook e continue acompanhando. Quando solicitado, iremos alternar para a sessão SQL do Databricks e executar as consultas.
-- MAGIC
-- MAGIC   A célula a seguir gera uma instrução de consulta totalmente qualificada que especifica todos os três níveis para a exibição, pois a executaremos em um ambiente que não possui variáveis ou um catálogo e schema padrão configurados. Execute a consulta gerada abaixo na sessão SQL do Databricks. Como todas as concessões apropriadas estão disponíveis para os analistas acessarem a view, a saída deve se parecer com o que vimos anteriormente ao consultar a view gold.

-- COMMAND ----------

SELECT "SELECT * FROM ${DA.my_new_catalog}.example.gold" AS Query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Consultar uma tabela como um analista
-- MAGIC De volta a mesma consulta na sessão SQL do Databricks, vamos substituir *gold* por *silver* e executar a consulta. Desta vez, ela falha, porque nunca configuramos permissões na tabela *silver*.
-- MAGIC
-- MAGIC Consultar a *gold* funciona porque a consulta representada por uma view é essencialmente executada como o proprietário da view. Essa propriedade importante permite alguns casos de uso de segurança interessantes. Dessa forma, as views podem fornecer aos usuários uma visão restrita de dados confidenciais, sem fornecer acesso aos próprios dados subjacentes.
-- MAGIC
-- MAGIC Por enquanto, você pode fechar e descartar a consulta *silver* na sessão Databricks SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criar e conceder acesso a uma função definida pelo usuário (UDF)
-- MAGIC O Unity Catalog é capaz de gerenciar UDFs dentro de schemas. O código a seguir configura uma função simples que mascara todos, exceto os dois últimos caracteres de uma string, e então a testa. Mais uma vez, somos os proprietários dos dados, portanto, nenhuma concessão é necessária.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION mask(x STRING)
  RETURNS STRING
  RETURN CONCAT(REPEAT("*", LENGTH(x) - 2), RIGHT(x, 2)
); 
SELECT mask('sensitive data') AS data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Para permitir que membros do grupo *analysts* executem nossa função, eles precisam das permissões **EXECUTE** na função, juntamente com a permissão **USAGE** no schema e no catálogo que mencionamos anteriormente.

-- COMMAND ----------

GRANT EXECUTE ON FUNCTION mask to analysts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Execute a função como um analista
-- MAGIC Cole a instrução de consulta totalmente qualificada gerada abaixo em uma nova consulta para executar esta função na sessão SQL do Databricks. Como todas as concessões apropriadas estão disponíveis para os analistas acessarem a função, a saída deve se parecer com o que acabamos de ver acima.

-- COMMAND ----------

SELECT "SELECT ${DA.my_new_catalog}.example.mask('sensitive data') AS data" AS Query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Proteger colunas e linhas das tabelas com dynamic views
-- MAGIC Vimos que o tratamento de views do Unity Catalog fornece a capacidade para as views protegerem o acesso às tabelas. Os usuários podem ter acesso a views que eles manipulam, tranformam ou ocultam dados de uma tabela de origem, sem precisar fornecer acesso direto à tabela de origem.
-- MAGIC
-- MAGIC As views dinâmicas fornecem a capacidade de fazer um controle de acesso refinado de colunas e linhas em uma tabela, condicional à entidade que executa a consulta. As views dinâmicas são uma extenção das views padrão que nos permitem fazer as coisas como:
-- MAGIC - obscurecer parcialmente os valores da coluna ou eliminá-los totalmente
-- MAGIC - omitir linhas baseadas em um critério específico
-- MAGIC
-- MAGIC O controle de acesso com views dinâmicas é obtido por meio do uso de funções dentro da definição da view. Essas funções incluem:
-- MAGIC * **`current_user()`**: retorna o endereço de email do usuário que está consultando a view
-- MAGIC * **`is_account_group_member()`**: retorna TRUE se o usuário que está consultando a view é um membro do grupo especificado
-- MAGIC
-- MAGIC Observação: evite usar a função herdada **`is_member()`**, que faz referência a grupos no nível do workspace. Esta é uma boa prática no contexto do Unity Catalog.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Editar/Redefinir (redact) colunas
-- MAGIC Suponha que queremos que os analistas possam ver as tendências de dados agregados da view *gold*, mas não queremos divulgar o usuário. Vamos redefinir a visualização para redigir as colunas *mrn* e *name* usando **`is_account_group_member()`**.
-- MAGIC
-- MAGIC Observação: este é um exemplo de treinamento simples que não necessariamente se alinha com as práticas recomendadas gerais. Para um sistema de produção, uma abordagem mais segura seria redigir os valores de coluna para todos os usuários que não são membros de um grupo específico.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold AS
SELECT
  CASE WHEN
    is_account_group_member('analysts') THEN 'REDACTED'
    ELSE mrn
  END AS mrn,
  CASE WHEN
    is_account_group_member('analysts') THEN 'REDACTED'
    ELSE name
  END AS name,
  MEAN(heartrate) avg_heartrate,
  DATE_TRUNC("DD", time) date
  FROM silver
  GROUP BY mrn, name, DATE_TRUNC("DD", time);

-- Re-issue the grant --
GRANT SELECT ON VIEW gold to analysts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Vamos consultar a view.

-- COMMAND ----------

SELECT * FROM gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Para nós, isso gera uma saída não filtrada, pois não somos membros do grupo *analysts*. Agora, revisite a sessão SQL do Databricks e execute novamente a consulta na view *gold* como um analista. Veremos que os valores das colunas *mrn* e *name* foram editados.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Restringir linhas
-- MAGIC Agora vamos supor que queremos uma view que, em vez de agregar e editar as colunas, ela simplesmente filtre as linhas da origem. Vamos aplicar a mesma função **`is_account_group_member()`** para criar uma view que passe apenas por linhas cujo *device_id* seja menor que 30. A filtragem de linha é feita aplicando uma condicional como uma cláusula **WHERE**.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold AS
SELECT
  mrn,
  time,
  device_id,
  heartrate
FROM silver
WHERE
  CASE WHEN
    is_account_group_member('analysts') THEN device_id < 30
    ELSE TRUE
  END;

-- Re-issue the grant --
GRANT SELECT ON VIEW gold to analysts

-- COMMAND ----------

SELECT * FROM gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Mascaramento de dados
-- MAGIC Um último caso de uso para views dinâmicas é o mascaramento de dados ou obscurecimento parcial dos dados. No primeiro exemplo, redigimos as colunas inteiramente. O mascaramento é semelhante em princípio, exceto que estamos exibindo alguns dos dados em vez de substituí-los totalmente. E para este exemplo simples, aproveitaremos a UDF *mask()* que criamos anteriormente para mascarar a coluna *mrn* para nossos analistas, embora o SQL forneça uma biblioteca bastante abrangente de funções de manipulação de dados integradas que podem ser aproveitadas para mascarar dados de várias maneiras diferentes. É uma boa prática aproveitá-los quando puder.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold AS
SELECT
  CASE WHEN
    is_account_group_member('analysts') THEN mask(mrn)
    ELSE mrn
  END AS mrn,
  time,
  device_id,
  heartrate
FROM silver
WHERE
  CASE WHEN
    is_account_group_member('analysts') THEN device_id < 30
    ELSE TRUE
  END;

-- Re-issue the grant --
GRANT SELECT ON VIEW gold to analysts

-- COMMAND ----------

SELECT * FROM gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Para nós, isso exige registros não pertubardos. Agora, revisite a sessão SQL do Databricks e execute novamente a consulta na view *gold* como analista. Todos os valores na coluna *mrn* serão mascarados.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Explorar os objetos
-- MAGIC Vamos explorar algumas instruções SQL para examinar nossos objetos de dados e permissões. Vamos começar fazendo um balanço dos objetos que temos no schema *examples*.

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW VIEWS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Nas duas instruções acima, não especificamos um schema, pois estamos contando com padrões que selecionamos. Alternativamente, poderíamos ter sido mais explícitos usando uma declaração como **`SHOW TABLES IN example`**.
-- MAGIC
-- MAGIC Agora vamos subir um nível na hierarquia e fazer um inventário dos schemas em nosso catálogo. Mais uma vez, estamos aproveitando o fato de termos um catálogo padrão selecionado. Se quiséssemos ser mais explícitos, poderíamos usar algo como **`SHOW SCHEMAS IN ${DA.my_new_catalog}`**.

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC O schema *example*, é aquele que criamos anteriormente. O schema padrão é criado de acordo com as convenções SQL ao criar um novo catálogo.
-- MAGIC
-- MAGIC Por fim, vamos listar os catálogos em nosso metastore.

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Pode haver mais entradas do que você. No mínimo você verá:
-- MAGIC - Um catálogo começando com o prefixo *dbacademy_*, que é o que criamos anteriormente.
-- MAGIC - *hive_metastore*, que não é um catálogo real no metastore, mas sim uma representação virtual do metastore *Hive* local do workspace. Use isso para acessar tabelas e views locais do workspace.
-- MAGIC - *main*, um catálogo criado por padrão a cada novo metastore
-- MAGIC - *samples*, outro catálogo virtual que apresenta conjuntos de dados de exemplo fornevidos pelo Databricks
-- MAGIC
-- MAGIC Pode haver mais catálogos presentes dependendo da atividade histórica em seu metastore.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Explorar as permissões
-- MAGIC Agora vamos explorar as permissões usando **`SHOW GRANTS`**.

-- COMMAND ----------

SHOW GRANTS ON VIEW gold

-- COMMAND ----------

SHOW GRANTS ON TABLE silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Somente nós, os proprietários dos dados, podemos acessar esta tabela diretamente. Qualquer pessoa com permissão para acessar a view *gold*, da qual também somos os proprietários dos dados, pode acessar esta tabela indiretamente.

-- COMMAND ----------

SHOW GRANTS ON SCHEMA example

-- COMMAND ----------

SHOW GRANTS ON CATALOG ${DA.my_new_catalog}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Revogar os acessos
-- MAGIC Nenhuma plataforma de governança de dados estaria completa sem a acapacidade de revogar concessões emitidas anteriormente. Vamos começar exeminando o acesso à função *mark()*.

-- COMMAND ----------

SHOW GRANTS ON FUNCTION mask

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora vamos revogar esta permissão de acesso.

-- COMMAND ----------

REVOKE EXECUTE ON FUNCTION mask FROM analysts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora vamos reexaminar o acesso, que agora estará vazio.

-- COMMAND ----------

SHOW GRANTS ON FUNCTION mask

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Revisite a sessão SQL do Databricks e execute novamente a consulta na view *gold* como analista. Observe que isso ainda funciona como antes.
-- MAGIC
-- MAGIC Lembre-se de que a view está efetivamente sendo executada como um proprietário, que também possui a função e a tabela de origem. Assim como o analista não exigia acesso direto à tabela que está sendo consultada, pois o proprietário da view é proprietário da tabela, a função continua a funcionar pelo mesmo motivo.
-- MAGIC
-- MAGIC Agora vamos tentar algo diferente. Vamos quebrar a cadeia de permissão revogando **USAGE** no catálogo.

-- COMMAND ----------

REVOKE USAGE ON CATALOG ${DA.my_new_catalog} FROM analysts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC De volta ao Databricks SQL, execute novamente a consulta *gold* como analista e vemos agora que, embora tenhamos as permissões adequadas na view e no schema, o privilégio ausente no alto da hierarquia interromperá o acesso a esse recurso. Isso ilustra o modelo de permissão explícita do Unity Catalog em ação: **nenhuma permissão é implícita ou herdada.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Limpar
-- MAGIC Vamos executar a célula seguinte para remover o catálogo criado anteriormente. O **`CASCADE`** irá remover o catálogo e os elementos contidos nele. 

-- COMMAND ----------

USE CATALOG hive_metastore;
DROP CATALOG IF EXISTS ${DA.my_new_catalog} CASCADE;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
