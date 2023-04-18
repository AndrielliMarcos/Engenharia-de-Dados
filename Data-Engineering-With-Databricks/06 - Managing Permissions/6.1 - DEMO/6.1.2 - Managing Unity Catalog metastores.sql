-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gerenciando metastores no Unity Catalog
-- MAGIC Objetivos:
-- MAGIC - Criar e deletar metastores
-- MAGIC - Atribuir (assign) um metastore a um woekspace
-- MAGIC - Configurar metastore admins seguindo as práticas recomendadas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Visão Geral
-- MAGIC Um metastpre é um contêiner de nível superior de objetos no modelo de segurança do Unity Catalog e gerencia metadados e listas de controle de acesso para seus objetos.
-- MAGIC 
-- MAGIC Os account admins (administradores de conta) criam o metastore e os atribuem a workspaces para permitir que cargas de trabalho nesses workspaces acessem os dados representados no metastore. Isso pode se feito no account console, por meio de APIs REST ou usando Terraform. Nesta demosntração, exploraremos a criação e o gerenciamento de metastores de forma interativa usando o account console.
-- MAGIC 
-- MAGIC Existem alguns recursos de nuvem subjacentes que devem ser configurados primeiro pelo administrador de nuvem para oferecer suporte ao metastore. Isso inclui:
-- MAGIC - um contêiner de armazenamento em nuvem
-- MAGIC - uma credencial de nuvem que permite o Databricks acessar o contêiner

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Pré-requisitos
-- MAGIC Se quiser acompanhar esta demonstração, você irá precisar:
-- MAGIC - Recursos de account admin em sua conta Databricks, pois a criação de metastore é feita no account admin console
-- MAGIC - Resursos de nuvem conforme descrito no **Overview**, fornecida por seu cloud admin
-- MAGIC - Concluir os provedimentos descritos na demosntração *Gerebciamento de entidades no Unity Catalog* (especificamente você precisa de um grupo de *metastore_admins*)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criando um metastore
-- MAGIC Com nossos recursos de suporte prontos, vamos criar um metastore:
-- MAGIC 1. Faça o login na <a href="https://accounts.cloud.databricks.com/" target="_blank">account console</a> como account admin
-- MAGIC 1. Na barra lateral esquerda, clique em **Data**. Isso exibe uma lista de metastores criados atualmente
-- MAGIC 1. Clique em **Create metastore**
-- MAGIC 1. Vamos fornecer um nome para o metastore. Embora o nome deva ser exclusivo para sua conta, use qualquer convenção de nomenclatura que desejar. Os usuários não terão visibilidade do nome do mestore.
-- MAGIC 1. A configuração **Region** nos permite especificar a região na qual hospedar o metastore. Os metadados são mantidos no control plane (painel de controle) e devem se alinhar geograficamente com os workspaves aos quais esse mestore será atribuído, portanto, escolha uma região de acordo.
-- MAGIC **Observação: só pode haver um metastore por região.** Se a região desejada não estiver disponível, é possível que você já tenha um metastore criado lá. Se for esse o caso, use esse metastore e siga as práticas recomendadas usando catálogos para segregar seus dados de acordo com suas necessidades.
-- MAGIC 1. Especifique a path do contêiner de armazenamento em nuvem, conforme fornecido por seu administrador de nuvem. Os arquivos de dados da tabela gerenciada serão armazenados neste local.
-- MAGIC 1. Especifique a credencial para acessar o contêiner de armazenamento, conforme fornecido por seu administrador de nuvem.
-- MAGIC 1. Finalmente clique em **Create**
-- MAGIC 
-- MAGIC A partir daqui, podemos atribuir o metastore recém criado a qualquer workspace disponpivel nesta conta. Mas, por enquanto, vamos clicar em **Skip**, pois isso pode ser feito a qualquer momento no futuro.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Deletando um metastore
-- MAGIC Quando metastores não são mais necessários, podemos excluí-los. Observe que esta operação exclui apenas os metadados associados ao metastore. Você deve verificar se o bucket está limpo para eliminar os dados das tabelas gerenciadas.
-- MAGIC 1. Na página **Data**, localize e selecione o metastore alvo.Use o campo **Search** se desejar
-- MAGIC 1. Clique nos três pontos no canto superior direito da página e selecione **Delete** (você será solicitado a confirmar, mas por enquanto você pode cancelar)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Gerenciando atribuições (assigments) de workspace
-- MAGIC Para que os recursos de computação acessem um metastore, ele deve ser atribuído a um workspace. Observe as seguintes regras relacionadas à atribuição de metastore:
-- MAGIC - Um metastore deve estar localizado na mesma região que o espaço de trabalho
-- MAGIC - Um metastore pode ser atribuído (assigned) a vários wokspaces (na mesma região)
-- MAGIC - Um workspace pode ter apenas um metastore atribuído a qualquer momento

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Atribuindo um metastore a um workspace
-- MAGIC A atribuição de metastore pode ser feita a qualquer momento, embora tome cuidado para que nenhuma carga de trabalho esteja em execução que ser perturbada. Para atribuir:
-- MAGIC 1. Na página **Data** do account console, vamos selecionar o metastore que queremos atribuir (usando o campo **Search** se desejar)
-- MAGIC 1. Selecione a aba **Workspace**. Isso mostra uma lista de workspaces aos quais o metastore está atribuído no momento.
-- MAGIC 1. Clique em **Assign to workspace**
-- MAGIC 1. Selecione o(s) workspace(s) desejado(s)
-- MAGIC 1. Clique em **Assign**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Removendo um metastore de um workspace
-- MAGIC Se precisar trocar um metastore, o processo é semelhante. Podemos desanexar o metastore de qualquer workspace atualmente atribuído.
-- MAGIC 1. Na página **Data**, selecione o metastore que queremos tirar a atribuição (usando o campo **Search** se desejar)
-- MAGIC 1. Selecione a aba **Workspace**. Isso mostra uma lista de workspaces aos quais o metastore está atribuído no momento.
-- MAGIC 1. Localize o workspace usando o campo **Search** se desejar, e clique nos três pontos na coluna mais à direita.
-- MAGIC 1. Selecione **Remove from this metastore** (você será solicitado a confirmar, mas por enquanto você pode cancelar)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Transferindo a administração do metastore
-- MAGIC Por padrão, o account admin que criou um metastore é o administrador do metastore. As responsabilidades do metastore admin são numerosas e incluem:
-- MAGIC - Criar e atribuir permissões em catálogos
-- MAGIC - Criar e remover achemas, tabelas e views se outros usuários não tiverem permissão para fazê-lo de acordo com suas políticas de governança de dados
-- MAGIC - Integrar o armazenamento externo no metastore
-- MAGIC - Configurar compartilhamentos com Delta Sharing
-- MAGIC - Manutenção geral e tarefas administrativas em objetos de dados quando seus proprietários não são acessíveis. Por exemplo, tranferir a propriedade de um schema petencente a um usuário que deixou a organização.
-- MAGIC 
-- MAGIC Os account admins provavelmente já são indivíduos ocupados. Portanto, para evitar gargalos em seus processos de governança de dados, é importante conceder administração de metastore a um grupo para que qualquer pessoa do grupo possa assumir essa tarefas. À medida que as funções mudam, os usuários podem ser facilmente adicionados ou removidos desse grupo.
-- MAGIC 1. N página **Data**, selecione o metastore alvo (usando o campo **Search** se desejar)
-- MAGIC 1. Localize o campo **Owner** que exibe o administrador. Atualmente, este é o indivíduo na organização que criou originalmente o metastore.
-- MAGIC 1. Selecione o link **Edit** à direita de **Owner**
-- MAGIC 1. Escolha uma entidade que se tornará o novo administrador. Vamos usar o grupo *metastore_admins*
-- MAGIC 1. Clique em **Save**
-- MAGIC 
-- MAGIC Agora qualquer pessoa desse grupo poderá realizar tarefas administrativas no metastore.
