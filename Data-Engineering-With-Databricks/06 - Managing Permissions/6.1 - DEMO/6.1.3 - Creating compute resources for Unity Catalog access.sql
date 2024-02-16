-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Criando clusters (compute sources) para acesso ao Unity Catalog
-- MAGIC Nesta demonstração, você aprenderá como: 
-- MAGIC - Configurar um cluster para acesso ao Unity Catalog
-- MAGIC - Configurar um SQL warehouse para acessar o Unity Catalog
-- MAGIC - Usar o Data Explorer para navegar pelos namespace de três níveis do Unity Catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Visão Geral
-- MAGIC Antes do Unity Catalog, uma solução completa de governança de dados exigia uma configuração cuidadosa das configurações do workspace, listas de controle de acesso e configurações de políticas do cluster. Era um sistema cooperativo que, se não for configurado corretamente, poderia permitir que os usuários contornassem completamente o controle de acesso.
-- MAGIC
-- MAGIC Embora o Unity Catalog apresente algumas novas configurações, o sistema não requer nenhuma configuração específica para ser seguro. Sem as configurações adequadas, os clusters não poderão acessar nenhum dado protegido, tornando o Unity Catalog seguro por padrão. Isso, juntamente com seu controle e auditoria aprimorados e refinados, torna o Unity Catalog uma solução de governança de dados segnificativamente evoluída.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Pré-requisitos
-- MAGIC Se quiser acompanhar esta demonstração, você precisará do direito **Allow unrestricted cluster creation** (permissão irrestrira para criação de um cluster). Consulte o workspace admin se você não tiver a capacidade de criar clusters ou warehouses SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data Science e Engineering Workspace
-- MAGIC Além dos SQL warehouses, os cluster são a porta de entrada para os seus dados, pois são os workhorses responsáveis por executar o código em seus notebooks. Nesta seção, vemos como configurar um cluster multifuncional para acessar o Unity Catalog. Os mesmos princípios de configuração podem ser aplicados ao configurar clusters de tarefas para executar cargas de trabalho automatizadas.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criando um cluster
-- MAGIC Vamos criar um cluster all-purpose capaz de acessar o Unity Catalog. Para usuários existentes do Databricks, esse procedimento será familiar, mas exploramos algumas novas configurações.
-- MAGIC 1. No workspace Data Science & Engineering, clique em **Compute** na barra lateral esquerda
-- MAGIC 1. Clique em **Create Cluster**
-- MAGIC 1. Vamos especificar um ome para nosso cluster. Deve ser único no workspace
-- MAGIC 1. Agora vamos dar uma olhada na configuração do **Cluster mode**. Observe que, com o Unity Catalog, a opção *High Concurrency*, que os usuários existentes no Databricks podem ter usado no passado, foi depreciada. Um dos principais casos de uso por trás dessa opção está relacionado ao controle de acesso à tabela antes do Unity Vatalog, mas um novo parâmetro chamado **Secury mode** foi introduzido para lidar com isso. Isso nos deixa com *Standard* e *Single node*, que você só precisa usar se tiver uma necessidade específica de um cluster Single node. Portanto, vamos deixar o o modo do cluster definido como *Standard*.
-- MAGIC 1. Escolha a **Databricks runtime version** como 11.1 ou mais alta
-- MAGIC 1. Com o objetivo de reduzir os custos de computação desta demonstração, desativarei o escalonamento automáticp e reduzirei o número de workers, embora não seja necessário fazer isso.
-- MAGIC 1. Agora devemos configurar o **Security mode**, que está acessível nas opções avançadas. Existem cinco opções disponíveis, mas três delas não são compatíveis com o Unity Catalog. As duas opções que precisamos considerar são as seguintes:
-- MAGIC     - Cluster *User isolation* pode ser compartilhado por vários usuários, mas a funcionalidade geral é reduzida para promover um ambiente seguro e isolado. Somente Python e SQL são permitidos, e alguns recursos avançados de cluster, como instalação de biblioteca, scripts de inicialização e montagem DBFS Fuse, também não estão disponíveis.
-- MAGIC     - Cluster *Single user* suporta todas as linguagens e recursos, mas o cluster só pode ser usado exclusivamente por um único usuário(por padrão, o proprietário do cluster). Esta é a escolha recomendada para job cluster e clusters interativos, se você precisar de alguns recursos não oferecidos no modo *User isolation*
-- MAGIC     Vamos escolher *Single user*. Observe que quando escolhemos este modo, um novo campo chamado **Single user access** aparece
-- MAGIC 1. A configuração **Single user access** nos permite sedignar quem pode se conectar a este cluster. Essa configuração é independente e não está relacionada aos controles de acesso ao cluster, com os quais os usuários existentes do Databricks podem estar familiarizados. Essa configuração é aplicada no cluster e somente o usuário designado aqui poderá se conectar. Todos os outros serão rejeitados. Por padrão, isso é definido para nós mesmos, o que deixaremos como está. Mas considere alterar essa configuração quando uma das seguintes situações se aplicar:
-- MAGIC     - Você está criando um cluster all-purpose para outra pessoa
-- MAGIC     - Você está configurando um job cluster, caso em que deverá ser definido para um service principal que executará o trabalho
-- MAGIC 1. Clique em **Create cluster.** Esta operação leva alguns instantes, então vamos esperar nosso cluster iniciar 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Dados de navegação
-- MAGIC Vamos dedicar alguns momentos para nos familiarizarmos com a página **Data** atualizada no workspace Data Science e Engineering.
-- MAGIC 1. Clique em **Data** na barra lateral esquerda. Observe que agora somos apresentados a três colunas, onde anteriormente havia duas:
-- MAGIC     - **Catalogs** nos permite selecionar um catálogo, o contêiner de nível superior na hierarquia de objetos de dados do Unity Catalog e a primeira parte do namespace de três níveis
-- MAGIC     - **Databases** também conhecidos como schemas, permite selecionar um schema dentro do catálogo selecionado. Esta é a segunda parte do namespace de três níveis (ou a primeira parte do namespace tradicional de dois níveis com a qual a maioria estará familiarizada)
-- MAGIC     - **Tables** nos permite selecionar uma tabela para explorar. A partir daqui, podemos visualizar os metadados e o histórico de uma tabela e obter dados de mostra
-- MAGIC 1. Vamos explorar a hierarquia. Os catálogos, schemas e tabelas que você pode selecionar dependem de quão populoso é seu metastore e das permissões
-- MAGIC 1. O catálogo *main* é criado por padrão como parte do processo de criação de metastore (semelhante a um schema padrão)
-- MAGIC 1. O catálogo *hive_metastore* corresponde ao metastore do Hive local para o workspace 
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Explorar os dados usando um notebook
-- MAGIC 1. Vamos criar um novo notebook SQL no workspace para executar um teste. Vamos anexar o notebook em um cluster que criamos
-- MAGIC 1. Criar uma nova célula com a consulta `SHOW GRANTS ON SCHEMA main.default` e executá-la

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Databricks SQL
-- MAGIC SQL warehouses são os outros meios principais para acessar seus dados, pois são os recursos de computação responsáveis por executar suas consultas SQL Databricks. Se você desejar habilitar a conectividade com ferramentas externas de BI, também precisará canalizá-las por meio de um SQL warehouse.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criando um SQL warehouse
-- MAGIC Como os SQL warehouses são configurações de cluster criadas especificamente, a configuração do Unity Catalog é mais fácil:
-- MAGIC 1. Alterne para a persona **SQL**
-- MAGIC 1. Clique em **SQL Warehouses** na barra lateral esquerda
-- MAGIC 1. Clique em **Create SQL Warehouse**
-- MAGIC 1. Vamos especificar um nome para nosso warehouse. Isso deve ser exclusivo no workspace
-- MAGIC 1. Com o objetivo de reduzir o custo desse ambiente de laboratório, escolherei um tamanho de cluster 2X-Small e também definirei o dimensionamento máximo como 1
-- MAGIC 1. Em **Advanced options**, garanta que o ** Unity Catalog** esteja habilitado
-- MAGIC 1. Com as configurações completas, clique em **Create**
-- MAGIC 1. Vamos tornar o cluster acessível a todos no espaço de trabalho. Na caixa de diálogo **Manage permissions**, vamos adicionar uma nova permissão:
-- MAGIC     - Clique no campo *search* e select *All users*
-- MAGIC     - Deixe a permissão definida como *Can use* e clique em **Add**
-- MAGIC
-- MAGIC Esta operação leva alguns instantes, então vamos esperar que nosso warehouse comece.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Dados de navegação
-- MAGIC Vamos dedicar alguns momentos para nos familiarizarmos com a página **Data Explore**
-- MAGIC 1. Clique em **Data** na barra lateral a esquerda. O layout da interface do usuário é diferente do Data Science e Engineering workspace. Observe como nos é apresentada uma visão hierárquica do metastore, ainda apresentando os mesmo elementos
-- MAGIC 1. Vamos explorar a hierarquia. Como antes, os catálogos, schemas e tabelas que você pode selecionar dependem de como seu metastore está preenchido e das permissões.
-- MAGIC 1. A partir daqui também podemos criar objetos ou gerenciar permissões, porém reservaremos essas tarefas para um laboratório posterior

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Explorando dados usando queries
-- MAGIC Vamos repetir as consultas que fizemos anteriormente no workspace Data Science e Engineering, desta vez crinaod uma nova consulta DBSQL e executando-a usando o SQL warehouse que acabamos de criar: `SHOW GRANTS ON SCHEMA main.default`.
