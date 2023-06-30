-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Gerenciamento de entidades no Unity Catalog
-- MAGIC Neste notebook iremos aprender como:
-- MAGIC - Criar identities (identidades) no account console para usuários e service principals
-- MAGIC - Criar grupos e gerenciar os membros dos grupos
-- MAGIC - Acessar recursos de gerenciamento de identity

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Visão Geral
-- MAGIC As identities do Databricks existem em dois níveis. Os usuários existentes do Databricks provavelmente estão familiarizados com as identities no nível do workspace, que foram e continuam sendo vinculadas às credenciais usadas para acessar os serviços do Databricks, como o workspace de Data Science e Engineering, Databricks SQL e Databricks Machine Learning. Antes do Unity Catalog, as identities em nível de conta tinham pouca relevância para a maioria dos usuários, pois essas identities eram usadas apenas para administrar a account (conta) do Databricks. Com a introdução do Unity Catalog e sua situação fora do workspace, fazia sentido enraizar as identities que ele usa no nível de account. Portanto, é importante entender a distinção entre esses dois níveis de identity e como gerenciar o relacionamento entre os dois.
-- MAGIC
-- MAGIC As identities em nível de account, nas quais focamos nesta demontração, são gerenciadas por meio do account console do Databricks ou de suas APIs SCIM associadas. Nesta demonstração, vamos focar no uso do account console, mas veremos os pontos de integração para automatizar o gerenciamento de identities.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Pré-requisitos
-- MAGIC Se você quiser acompanhar esta demonstração, precisará de recursos de account administrator (administrador de conta) em sua conta do Databricks, pois a criação de identities é feita no console do account admin (administrador de conta).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Gerenciando usuários e service principals
-- MAGIC No Databricks, um **usuário** corresponde a um indivíduo que usa o sistema de forma interativa - ou seja, uma pessoa. O usuários são identificados através do endereço de email deles e autenticados usando esse endereço de email e uma senha que eles gerenciam. Os usuários interagem com a plataforma através da inteface do usuário e eles também podem acessar a funcionalidade por meio de ferramentas de linha de comando e APIs REST. A prática recomendada determina que eles gerem um token de acesso pessoal (PAT) para autenticar essas ferramentas.
-- MAGIC
-- MAGIC Somente os poucos usuários que são administradores de conta realmente farão login na accont console. O restante fará login em um dos workspaces aos quais foram atribuídos. Porém, sua identity no nível da account ainda é crítica para permitir o acesso aos dados por meio do Unity Catalog.
-- MAGIC
-- MAGIC Um **service pricipal** (entidade de serviço) é um tipo diferente de identity individual destinada ao uso com ferramentas de automação e jobs em execução. Eles recebem um nome de account admin, embora sejam identificados por meio de um identificador exclusivo global (GUID) que é gerado dinamicamente quando a identidade é criada. Os Service Principals são autenticados em um espaço de trabalho usando um token e acessam a funcionalidade através de API.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criando um usuário
-- MAGIC Vamos adicionar um novo usuário na nossa account:
-- MAGIC 1. Faça login no <a href="https://accounts.cloud.databricks.com/" target="_blank">account console</a> como account admin
-- MAGIC 1. Na barra lateral esquerda, clique em **User management**. A aba **Users** é mostrada por padrão.
-- MAGIC 1. Clique em **Add user**
-- MAGIC 1. Forneça um endereço de email. Esta é a informação de identificação que identifica exclusivamente os usuários em todo o sistema. Deve ser um email válido, pois será utilizado para confirmar a identity e gerenciar a senha. Para fins de demonstração, pode-se criar rapidamente endereços de email temporários usando dispostable.com.
-- MAGIC 1. Forneça o nome e sobrenome. Embora estes nomes não sejam usados pelo sistema, eles tornam as identities mais legíveis por humanos.
-- MAGIC 1. Clique em **Send invite.
-- MAGIC 1. O novo usuário receberá um email convidando-o a ingressar e definir sua senha.
-- MAGIC
-- MAGIC Embora este usuário tenha sido adicionado à conta, ele ainda não poderá acessar os serviços do Databricks, pois não foi atribuído a nenhum workspace. No entanto, sua adição à conta os torna um titular válido ais olhos do Unity Catalog.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Validando um usuário
-- MAGIC Vamos verificar se nosso usuário recém-criado pode ser visto pelo Unity Catalog. Podemos fazer isso com o Data Explorer, disponível no Databricks SQL.
-- MAGIC 1. No account console, vamos abrir um dos workspaces disponíveis.
-- MAGIC 1. Agora vamos mudar para a persona SQL
-- MAGIC 1. Na barra lateral, clique em **Data**
-- MAGIC 1. No painel **Data**, vemos uma lista de catálogos. Isso nos dá nosso primeiro vislumbre do namespace de três níveis em ação. Selecione um dos catálogos, exceto **hive_metastore** ou **samples**. **Main** geralmente é uma boa escolha, pois é criado por padrão com novos metastores
-- MAGIC 1. Selecione a aba **Permissions**
-- MAGIC 1. Clique em **Grant**
-- MAGIC 1. Faça uma pesquisa digitando algum elemento da identity que acabamos de criar (nome, endereço de email, nome de domínio)
-- MAGIC
-- MAGIC Observe como a identity aparece na lista supensa como uma ententity selecionável. Isso sifnifica que essa identificação é uma identity válida no que diz respeito ao Unity Catalog. Isso também significa que podemos começar a conceder privilégios em objetos de dados a esse usuário. No entendo, esse usuário ainda não pode fazer nada, pois não está atribuído a nenhum workspace.
-- MAGIC
-- MAGIC **Observação: apesar do que foi dito, é recomendável gerenciar permissões em objetos de dados usando grupos, como discutiremos com mais detalhes em breve. Esta prática leva a um sistema que é muito mais fácil de manter.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Deletando um usuário
-- MAGIC Se um usuário deicar a organização nós podemos deletar ele sem afetar qualquer objeto de dados que ele possua. Se eles estiverem ausentes por um período prolongado, mas desejamos revogar temporariamente o acesso, os usuários também podem ser desativados (embora essa opção esteja disponível apenas por meio da API no momento).
-- MAGIC
-- MAGIC Como deletar um usuário:
-- MAGIC 1. Na página **User management** do account console, localize e selecione o usuário de destino (usando o vampo **Search**, se desejar)
-- MAGIC 1. Clique nos três pontos no canto superior direito da página e selecione **Delete user** (você será solicitado a confirmar, mas por enquanto você pode cancelar)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Service principals (entidades de serviço)
-- MAGIC O workflow para gerenciar service principal é praticamente idêntico aos usuários:
-- MAGIC 1. Na página **User management** do account console, selecione a aba **Service principals**
-- MAGIC 1. Clique em **Add service principal**
-- MAGIC 1. Forneça um nome. Embora esa não seja uma informação de identificação, é úti, usar algo que faça sentido para os administradores
-- MAGIC 1. Clique Add
-- MAGIC
-- MAGIC Service principals são identificados pelo **Aplication ID** deles.
-- MAGIC
-- MAGIC Para deletar um service principal:
-- MAGIC 1. Na aba **Service Principals**, localize e selecione o service principal desejado na lista. Use o campo **Search** se necessário
-- MAGIC 1. Clique nos três pontos no canto superior direito da página e selecione **Delete** (você será solicitado a confirmar, mas por enquanto você pode cancelar)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Automatizando o gerenciamento de usuários
-- MAGIC O Databricks suporta a API SCIM e pode integrar com um provedor de identity externo para automatizar a criação de usuários e contas. Essa integração traz alguns benefícios importantes:
-- MAGIC * Libera o account admin das responsabilidades de gerenciamento de usuários e grupos
-- MAGIC * Simplifica a sincronização. Embora o Databricks mantenha suas próprias identities para seus usuários, a integração do provedor de identity pode gerar automaticamente os usuários em nome do administrador
-- MAGIC * Simplifica a experiência do usuário acessando o Databricks por meio do SSO da sua organização
-- MAGIC
-- MAGIC A configuração está fora do escopo deste treinamento, no entanto, aqui está como você pode acessar esses recursos:
-- MAGIC 1. Vamos clicar no ícone **Settings** na barra lateral esquerda da account console
-- MAGIC 1. Consulte as abas **Single sign-on** e **User Provisioning tabs**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Gerenciando Grupos
-- MAGIC O conceito de grupos é difundido em inúmeros modelos de segunça e por boas razões. Os grupos reúnem usuários individuais (e service principals) em unidades lógicas para simplificar o gerenciamento. Os grupos também podem ser aninhados dentro de outros grupos, se necessário. Quaisquer concessões no grupo são automaticamente herdadas por todos os membros do grupo.
-- MAGIC
-- MAGIC As políticas de governança de dados são definidas em termos de funções, e os grupos fornecem uma construção de gerenciamento de usuários que mapeia bem essas funções, simplificando a implementação dessas políticas de governança. Dessa forma, as permissões podem ser concedidas a grupos de acordo com as políticas de segurança de sua organização e os usuários podem ser adicionados a grupos de acordo com suas funções dentro da organização.
-- MAGIC
-- MAGIC Quando os usuários fazem a transição entre funções, é simples mover um usuário de um grupo para outro. Executar uma operação equivalente quando as permissões são conectadas no nível do usuário individual é significativamente mais intensivo. Da mesma forma, à medida que seu modelo de governança evolui e as definições de função mudam, é muito mais fácil efetuar essas alterações nos grupos em vez de replicar a alterações em vários usuários individuais.
-- MAGIC
-- MAGIC Por esses motivos, recomendamos implementar grupos e conceder permissões de dados a grupos em vez de usuários individuais ou entidades de serviço.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criando um grupo
-- MAGIC 1. Na página **User management** do account console, selecione a aba **Groups**
-- MAGIC 1. Clique em **Add group**
-- MAGIC 1. Dê um nome para o grupo (por exemplo *analysts*)
-- MAGIC 1. Clique **Save**
-- MAGIC
-- MAGIC Vamos repetir o processo para criar outro novo grupo chamado *metastore_admins*.
-- MAGIC A partir daqui podemos adicionar imediatamente membros ao novo grupo, ou é uma tarefa que ser realizada a qualquer momento.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Gerenciando membros no grupo
-- MAGIC Vamos adicionar o usuário que criamos anteriormente ao grupo *analysts* que acavamos de criar
-- MAGIC 1. Na aba **Groups**, localize e selecione o grupo *analysts*, usando o campo **Search** se desejar
-- MAGIC 1. Clique em **Add members**
-- MAGIC
-- MAGIC A partir daqui, podemos usar o campo de texto pesquisável que também funciona como um menu suspenso quando clicado. Vamos identificar os usuários, service principals ou grupos que queremos adicionar (procure e selecione o usuário criado anteriormente) e clique em **Add**. Observe que podemos adicionar vários deuma só vez, se necessário.
-- MAGIC
-- MAGIC A associação do usuário ao grupo entra em vigor imediatamente.
-- MAGIC
-- MAGIC Agora vamos repetir o processo para adicionar nós mesmo (não os usuários criados) ao grupo *metastore_admins* que acabamos de criar. A intenção desse grupo é simplificar o gerenciamento dos administradores do metastore, mas essa ação ainda não fará anada. Nós vamos chegar a isso na próxima seção.
-- MAGIC
-- MAGIC Remover um membro de um grupo é simples:
-- MAGIC 1. Localize e selecione o grupo que deseja gerenciar na aba **Groups**
-- MAGIC 1. Localize o membro, usando o campo **Search** se desejar
-- MAGIC 1. Clique nos três pontos na coluna mais à direita
-- MAGIC 1. Selecione **Remove** (você será solicitado a confirmar, mas por enquanto você pode cancelar)
-- MAGIC
-- MAGIC Todos os membros listados no grupo (incluindo grupos filhos) herdam automaticamente quaisquer concessões concedidas ao grupo. A atribuição de privilégios de maneira grupal como essa é considerada uma prática recomendada de governança de dados, pois simplifica bastante a implementação e a manutenção do modelo de segurança de uma orgacização.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Deletando um grupo
-- MAGIC À medida que seu modelo de governança de dados evolui, pode ser necessário eleminar grupos. A exclusão de grupos no Unity Catalog destrói a estrutura de associação e as permissões transmitidas, mas não excluirá recursivamente seus membros, nem afetará as permissões concedidas diretamente a esses indivíduos ou grupos filhos.
-- MAGIC
-- MAGIC Na barra lateral esquerda, clique em ** Users & Groups**
-- MAGIC 1. Na aba **Groups**, localize o grupo alvo, usando o campos **Search** se desejar
-- MAGIC 1. Clique nos três pontos na coluna mais a direita
-- MAGIC 1. Selevione **Delete** (você será solicitado a confirmar, mas por enquanto pode cancelar)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Gerenciando entidades de workspace
-- MAGIC Os account admin podem atribuir entidades (usuários e service principals individualmente ou grupos) a um ou mais workspaces. Se um usuário for atribuído a mais de um workspace, a barra lateral de navegação à esquerda apresentará a capacidade de alternar entre os workspaces aos quais els têm acesso.
-- MAGIC 1. Na account console, clique no ícone **Workspace** na barra lateral esquerda
-- MAGIC 1. Localize e selecione o workspace alvo, usando o campo **Search** se desejar
-- MAGIC 1. Selecione a aba **Permissions**
-- MAGIC 1. Uma lista de membros atualmente atribupidos é exibida. Para adicionar mais, vamos clicar em **Add permissions**
-- MAGIC 1. Pesquise um usuário, service principal ou grupo para atribuir (pesquise por *analysts*)
-- MAGIC 1. A lista suspensa **Pernission** oferece a opção de adicionar o membro como usuário regular ou um workspace admin. Deixe isso definido como *User*
-- MAGIC 1. Especifique membros adicionais, se desejar
-- MAGIC 1. clique em **Save**
-- MAGIC
-- MAGIC Os account admin também podem cancelar a atribuição de usuários ou service principal de um workspace.
-- MAGIC 1. Na página **Workspace**, localize e selecione o workspace alvo. Use o campo **Search** se desejar
-- MAGIC 1. Selecione a aba **Permissions**
-- MAGIC 1. Localize o membro desejado e clique nos três pontos na coluna mais a direita
-- MAGIC 1. Selecione **Remove** (você será solicitado a confirma, mas por enquanto você pode cancelar)
-- MAGIC
-- MAGIC **Observação:** os workspace admins podem administrar usuários nos workspaces, embora seja considerado uma prática recomendada gerenciar os membros no nível da conta.
