#!/usr/bin/env python
# coding: utf-8

# # Analisando dados de espécies de fauna em perigo de extinção no Brasil
# #### Por Fernando Sanjar Mazzilli, em Agosto de 2021

# ## Sumário
# 1. **[Introdução](#intro)**
# 2. **[Criando a Base de Dados](#etapa1)**
#     1. [Diagrama Inicial](#diagrama1)
# 3. **[Visualizando e filtrando dados com SQL](#etapa2)**
#     1. [Consulta para visualizar todas as espécies em risco em diferentes Unidades de Conservação ](#e2.1)
#     2.  [Usando a claúsula WHERE para filtrar resultados](#e2.2)
#     3.  [Usando expressões regulares no SQL (REGEX) para filtrar por Estado(s)](#e2.3)
# 4. **[Refinando Dados](#etapa3)**
#     1. [Importando novos dados da Internet](#e3.1)
#     2. [Detalhando variáveis já existentes](#e3.2)
#     3. [Diagrama Completo](#e3.3)
# 5. **[Contabilizando dados e cruzando informações](#etapa4)**
#     1. [Número de espécies ameaçadas por Unidade Federal (UF) e categoria de risco](#e4.1)
#     2. [Número de espécies ameaçadas por índices de IDH e Alfabetização](#e4.2)
#     3. [Número de espécies ameaçadas por Classe e Divisão](#e4.3)
#     4. [ Número de espécies ameaçadas por tipo de Unidade de Conservação](#e4.4)
# 6. **[6. Visualizações e gráficos](#etapa5)**
#     1. [Total de Espécies Ameaçadas por UF e Grau de Risco](#e5.1)
#     2. [Dispersão entre Número de Espécies em Risco e IDH ou Alfabetização](#e5.2)
#     3. [Distribuição de Espécies ameaçadas por divisão e Grau de Risco](#e5.3)
#     4. [Distribuição e Número de Espécies Ameaçadas por tipo de Unidade de Conservação](#e5.4)
# 7. **[Bibliografia](#biblio)**
# 

# <a name="intro"></a>
# ## 1. Introdução
# #### Este caderno é um experimento para importar e tratar dados armazenados em uma planilha do Microsoft Excel que contém informações sobre as espécies de fauna em risco de extinção nas principiais áreas de preservação e parques nacionais brasileiros.

# Os dados utilizados foram obtidos no [Portal Brasileiro de Dados Abertos](https://dados.gov.br/), e são de autoria da CNCFlora (2016). Consulte a bibliografia para acessá-los.
# <br><br>
# Neste caderno, meu objetivo primário foi limpar e organizar os dados armazenados na planilha Excel, que contém muitas strings repetidas em todas as colunas. Desta maneira, foi possível **reduzir o tamanho do conjunto de dados em cerca de 90%**. <br><br>
# Para isso, utilizei o framework **Pandas** para importar os dados da planilha Excel e manipulá-los utilizado a linguagem Python. Em seguida, armazenei todas as informações limpas em uma **base de dados SQLite**. Acredito que existam passos desnecessários e maneiras mais eficientes de tratar os dados utilizando apenas o Pandas, então feedbacks são muito bem-vindos! Vale lembrar que este é um **projeto de aprendizado**, e entendi que era mais interessante testar vários recursos diferentes.
# <br><br>
# Uma vez com os dados da fonte primária limpos e armazenados na base de dados, criei algumas visualizações simples usando **filtros, limitadores e expressões regulares no SQL**. Aproveitei para inserir dados adicionais na base a partir de informações extraídas diretamente da internet, utilizando o método _read_html_ da biblioteca Pandas
# 
# Por fim, fiz algumas análises simples de distribuição de espécies de fauna ameaçadas de extinção por grau de risco, Estado e Unidade de Preservação. Criei **visualizações e gráficos** utilizando as bibliotecas **_matplotlib_** e **_seaborn_**
# 

# <a name="etapa1"></a>
# ## 2. Criando a Base de Dados
# #### Carregando dependencias, fonte de dados e dataframe do Pandas

# In[1]:


import numpy as np
import pandas as pd
import sqlite3 as db
from IPython.display import display #para visualizar dados
src = input("Insira o nome da planilha, ou tecle Enter para usar o nome padrão: ")
if len(src) < 1:
    src = "fauna_fed.xlsx"
data = pd.read_excel(src)
print("Dados carregados como: " + str(type(data)))
display(data)


# #### Há muitas strings repetidas; vamos transformá-las em chaves primárias em tabelas específicas, para que possamos utilizá-las como chaves estrangeiras em outras tabelas:

# In[2]:


def criar_CP (fonte, nome_coluna, lista): #onde fonte é um dataframe Pandas e lista é uma variável onde serão armazenados os dicionários criados
    kp = dict()
    chave = 0
    for item in fonte[nome_coluna]:
        if item in kp:
            continue
        kp[item] = chave
        chave += 1 
    lista.append(kp)
    print("Chaves criadas para elementos da coluna: " + nome_coluna)

tabelas = list()
for col in data.columns[3:7]: # tabelas CP de divisão até família
    criar_CP(data, col, tabelas)
for col in data.columns[8:12]: # tabelas CP de espécies até Unidades Federais
    criar_CP(data, col, tabelas)
    
#obs: Poderíamos ter atingido um resultado semelhante utilizando o méotodo pd.core.frame.DataFrame.unique para cada uma das 
#     colunas, que retorna uma lista dos elementos únicos em uma coluna. A partir disso, poderíamos pedir para que o SQL 
#     criasse as chaves primárias automaticamente  para cada entrada adicionada na base de dados, bastando 
#     adicionar o argumento AUTOINCREMENT para as colunas "id" nas claúsulas CREATE escritas abaixo


# #### Criando base de dados SQLite e inserindo chaves primárias

# In[3]:


db.register_adapter(np.int32, int)  #Necessário para que o SQLite aceite ints com mais de 8 bits, 
db.register_adapter(np.int64, int)  #como é o caso das ints geradas pelo pandas

conn = db.connect("fauna_db.sqlite")
cur = conn.cursor()

cur.executescript("""
DROP TABLE IF EXISTS especie;
DROP TABLE IF EXISTS categoria;
DROP TABLE IF EXISTS divisao;
DROP TABLE IF EXISTS classe;
DROP TABLE IF EXISTS ordem;
DROP TABLE IF EXISTS familia;
DROP TABLE IF EXISTS UC;
DROP TABLE IF EXISTS UF;
DROP TABLE IF EXISTS risco;

CREATE TABLE risco (
    especie_id INTEGER,
    categoria_id INTEGER,
    UC_id INTEGER
);

CREATE TABLE especie (
    id  INTEGER NOT NULL PRIMARY KEY UNIQUE,
    nome    TEXT UNIQUE,    
    divisao_id INTEGER,
    classe_id INTEGER,
    familia_id INTEGER,
    ordem_id INTEGER
);

CREATE TABLE categoria (
    id  INTEGER NOT NULL PRIMARY KEY UNIQUE,
    nome    TEXT UNIQUE
);

CREATE TABLE divisao (
    id  INTEGER NOT NULL PRIMARY KEY UNIQUE,
    nome    TEXT UNIQUE
);

CREATE TABLE classe (
    id  INTEGER NOT NULL PRIMARY KEY UNIQUE,
    nome    TEXT UNIQUE
);

CREATE TABLE ordem (
    id  INTEGER NOT NULL PRIMARY KEY UNIQUE,
    nome    TEXT UNIQUE,
    classe_id INTEGER
);
CREATE TABLE familia (
    id  INTEGER NOT NULL PRIMARY KEY UNIQUE,
    nome    TEXT UNIQUE,
    ordem_id INTEGER
);
CREATE TABLE UC (
    id  INTEGER NOT NULL PRIMARY KEY UNIQUE,
    nome    TEXT UNIQUE,
    UF_id INTEGER
);
CREATE TABLE UF (
    id  INTEGER NOT NULL PRIMARY KEY UNIQUE,
    nome    TEXT UNIQUE
);

                 """)

titulos = ["divisao", "classe", "ordem", "familia", "especie", "categoria", "UC", "UF"]
i = 0
for tabela in tabelas:
    for nome in tabela:
        cur.execute("INSERT OR IGNORE INTO " +titulos[i] + " (id, nome) VALUES (?, ?)", (tabela[nome], nome, ))
    conn.commit()
    print("Chaves primárias inseridas na tabela \"" + titulos[i] + "\" do banco de dados")
    i += 1
    


# <a name="diagrama1"></a>
# ###  2.1 Diagrama Inicial
# ![diagrama-6.png](attachment:diagrama-6.png)
# <br>
# (Diagrama elaborado em: https://app.diagrams.net/)

# #### Mapeando chaves estrangeiras no dataframe pandas

# In[4]:


headers = list()
for col in data.columns[3:7]:
    headers.append(col)
for col in data.columns[8:12]:
    headers.append(col)
for i in range (len(tabelas)):  
    print ("Mapendo chaves estrangeiras na coluna:", headers[i])
    data[headers[i]] = data[headers[i]].map(tabelas[i]) #substitui todos os valores por suas respectivas chaves no dataset com base no dicionário presente em tabelas
display(data)


# #### Populando tabelas *risco*, *especie*, *familia* e *ordem* da base de dados:

# In[5]:


for i in range(len(data)):
    divisao_id = data[data.columns[3]][i]
    classe_id = data[data.columns[4]][i]
    ordem_id = data[data.columns[5]][i]
    familia_id = data[data.columns[6]][i]    
    especie_id = data[data.columns[8]][i]
    categoria_id = data[data.columns[9]][i]
    UC_id = data[data.columns[10]][i]
    UF_id = data[data.columns[11]][i]        
    cur.execute("""INSERT INTO risco (especie_id, categoria_id, UC_id) VALUES (?, ?, ?)""", (especie_id, categoria_id, UC_id, ))    
    cur.execute("""UPDATE especie SET divisao_id = ?, classe_id = ?, ordem_id = ?, familia_id= ? WHERE id = ? """, (divisao_id, classe_id, ordem_id, familia_id, especie_id,))
    cur.execute("""UPDATE familia SET ordem_id = ? WHERE id = ? """, (ordem_id, familia_id, ))
    cur.execute("""UPDATE ordem SET classe_id = ? WHERE id = ? """, (classe_id, ordem_id, ))  
    cur.execute("""UPDATE UC SET UF_id = ? WHERE id = ?""", (UF_id, UC_id, ))
conn.commit()
print("Tabelas populadas")


# <a name="etapa2"></a>
# ## 3. Visualizando e filtrando dados com SQL
# #### Carregando módulos para rodar consultas SQL no JupyterNotebooks e conectando base de dados

# In[6]:


get_ipython().run_line_magic('load_ext', 'sql')
get_ipython().run_line_magic('sql', 'sqlite:///fauna_db.sqlite')


# <a name="e2.1"></a>
# ### 3.1 Consulta (_querry_) para visualizar todas as espécies em risco em diferentes Unidades de Conservação :
# #### (10 primeiros resultados, organizados de forma descrescente em relação ao nome da classe)

# In[7]:


get_ipython().run_cell_magic('sql', '', '\nSELECT especie.nome as "Espécie", divisao.nome as "Divisão", classe.nome as "Classe", ordem.nome as "Ordem", familia.nome as "Família", categoria.nome as "Risco de Extinção", UC.nome as "Unidade de Conservação", UF.nome as "Estado(s)"  \nFROM risco JOIN especie JOIN divisao JOIN classe JOIN ordem JOIN familia JOIN categoria JOIN UC JOIN UF\nWHERE risco.especie_id = especie.id AND risco.categoria_id = categoria.id AND risco.UC_id = UC.id AND especie.divisao_id = divisao.id AND especie.familia_id = familia.id AND especie.classe_id = classe.id AND especie.ordem_id = ordem.id AND UC.UF_id = UF.id\nORDER BY classe.nome DESC\nLIMIT 10')


# <a name="e2.2"></a>
# ### 3.2 Usando a claúsula WHERE para filtrar resultados
# #### Vamos tentar visualizar apenas os anfíbios em risco de extinção:
# 
# Para isso, podemos seguir dois caminhos: **filtrar pela chave estrangeira** que chama a string "Amphibia" em nossa consulta, ou então **filtrar pela string** "Amphibia" diretamente.   
# Para filtrar pela chave estrangeira, primeiro devemos descobrir qual a **chave primária** associada à classe "Amphibia" na tabela classe:

# In[8]:


get_ipython().run_cell_magic('sql', '', '\nSELECT classe.id, classe.nome from classe WHERE classe.nome = "Amphibia"')


# Sabendo que a **chave primária** associada à classe Ampibia é **0**, podemos modificar a consulta inicial adicionando o seguinte argumento na cláusula **WHERE**:
# >classe.id = 0 

# In[9]:


get_ipython().run_cell_magic('sql', '', '\nSELECT especie.nome as "Espécie", divisao.nome as "Divisão", classe.nome as "Classe", ordem.nome as "Ordem", familia.nome as "Família", \n       categoria.nome as "Risco de Extinção", UC.nome as "Unidade de Conservação", UF.nome as "Estado(s)"  \nFROM   risco JOIN especie JOIN divisao JOIN classe JOIN ordem JOIN familia JOIN categoria JOIN UC JOIN UF\nWHERE  risco.especie_id = especie.id AND risco.categoria_id = categoria.id AND risco.UC_id = UC.id AND especie.divisao_id = divisao.id\n       AND especie.familia_id = familia.id AND especie.classe_id = classe.id AND especie.ordem_id = ordem.id AND UC.UF_id = UF.id \n       AND classe.id = 0\nLIMIT 10')


# Também podemos utilizar a string diretamente como filtro. Por exemplo: agora, entre os anfíbios, queremos saber apenas os que estão em risco de extinção na categoria CR. Podemos adicionar o seguinte argumento na cláusula **WHERE**:   
# > categoria.nome = "CR"
# 

# In[10]:


get_ipython().run_cell_magic('sql', '', '\nSELECT especie.nome as "Espécie", divisao.nome as "Divisão", classe.nome as "Classe", ordem.nome as "Ordem", familia.nome as "Família", \n       categoria.nome as "Risco de Extinção", UC.nome as "Unidade de Conservação", UF.nome as "Estado(s)"  \nFROM   risco JOIN especie JOIN divisao JOIN classe JOIN ordem JOIN familia JOIN categoria JOIN UC JOIN UF\nWHERE  risco.especie_id = especie.id AND risco.categoria_id = categoria.id AND risco.UC_id = UC.id AND especie.divisao_id = divisao.id\n       AND especie.familia_id = familia.id AND especie.classe_id = classe.id AND especie.ordem_id = ordem.id AND UC.UF_id = UF.id \n       AND classe.id = 0 AND categoria.nome = "CR"\nLIMIT 10')


# <a name="e2.3"></a>
# ###  3.3 Usando expressões regulares no SQL (REGEX) para filtrar por Estado(s)
# Na tabela UF, que contém as siglas dos estados brasileiros, há algumas entradas que possuem mais do que uma sigla (como MG/RJ). Isto acontece porque há parques que ocupam vários estados, e, na fonte de dados original, optou-se por incluir todos os estados separados por barras.   
# Para que isso não seja um problema no momento de filtrar as espécies em extinção por Estado, podemos utilizar expressões regulares para flexibilizar os filtros.    
# Por exemplo: se, para identificar todos os anfíbios em risco de extinção no estado de MG, adicionássemos à clausula **WHERE** 
# > UF.nome = "MG"
# 
# O SQL ignoraria todas as linhas com MG/RJ, pois a string "MG" que indicamos no filtro é diferente de "MG/RJ". Desta maneira, deixaríamos de recuperar resultados válidos para nosso filtro.   
# 
# Para evitar este problema, podemos adicionar como argumento na cláusula **WHERE**:
# >UF.nome like "%MG%"
# 
# Em REGEX, _**%**_ significa "qualquer string". _**like**_ indica ao SQL que usaremos uma REGEX. Desta maneira, a expressão acima significa "qualquer string seguida por MG seguida por qualquer string". Em outras palavras, basta que a string "MG" esteja contida na string armazenada no banco de dados para que o filtro seja ativado. Vejamos:

# In[11]:


get_ipython().run_cell_magic('sql', '', '\nSELECT especie.nome as "Espécie", divisao.nome as "Divisão", classe.nome as "Classe", ordem.nome as "Ordem", familia.nome as "Família", \n       categoria.nome as "Risco de Extinção", UC.nome as "Unidade de Conservação", UF.nome as "Estado(s)"  \nFROM   risco JOIN especie JOIN divisao JOIN classe JOIN ordem JOIN familia JOIN categoria JOIN UC JOIN UF\nWHERE  risco.especie_id = especie.id AND risco.categoria_id = categoria.id AND risco.UC_id = UC.id AND especie.divisao_id = divisao.id\n       AND especie.familia_id = familia.id AND especie.classe_id = classe.id AND especie.ordem_id = ordem.id AND UC.UF_id = UF.id \n       AND classe.id = 0 AND UF.nome like "%MG%"')


# <a name="etapa3"></a>
# ## 4. Refinando Dados
# 
# Nesta seção, vamos utilizar os dados armazenados em nossa base de dados para realizar análises, cruzando informações que temos em diferentes tabelas. Não precisamos, contudo, nos restringir aos dados presentes em nossa fonte original, como veremos a seguir:

# <a name="e3.1"></a>
# ### 4.1 Importando novos dados da Internet
# Antes de fazer análises, vamos garantir que todas as Unidades Federais possuem uma chave primária na tabela UF. Importaremos uma [tabela da Wikipedia](https://pt.wikipedia.org/wiki/Unidades_federativas_do_Brasil) que contém todas as siglas dos Estados utilizando o método _read_html_ do framework Pandas e indicando o filtro "Abreviação" para limitar os resultados

# In[12]:


wikitables = pd.read_html("https://pt.wikipedia.org/wiki/Unidades_federativas_do_Brasil", match = "Abreviação") # Retorna uma LISTA de dataframes Pandas
print(len(wikitables), "tabela(s) econtrada(s)")
wikitable = wikitables[0]
print("Tabela carregada na memória como", str(type(wikitable)))
display(wikitable)


# Além das **siglas dos estados**, esta tabela contém outras informações interessantes, como o **nome por extenso** da unidade federativa e informações de **IDH e alfabetização**. Podemos importá-las para nossa base de dados, também! Para isso, vamos **modificar a tabela UF em nosso banco de dados e adicionar 3 novas colunas**: nome_ext, idh e alfabetizacao. <br>
#  

# In[13]:


cur.executescript("""
ALTER TABLE UF ADD nome_ext TEXT;
ALTER TABLE UF ADD idh FLOAT;
ALTER TABLE UF ADD alfabetizacao FLOAT;
""")
print("Colunas adicionadas na tabela UF")


# Em seguida, vamos transportar os dados do dataframe pandas para Tuples do Python, que serão utlizadas para inserir os dados em nossa base de dados:

# In[14]:


newinfo = list()
for i in range (len(wikitable)):
    sigla = wikitable["Abreviação"][i]
    nome_ext = wikitable["Unidade federativa"][i]
    idh = wikitable["IDH (2010)"][i]
    alfab = (wikitable["Alfabetização (2016)"][i]).replace(",", ".")
    info = (sigla, nome_ext, idh, alfab)
    newinfo.append(info)   
print ("Extraídas as seguintes informações da tabela:\n", newinfo)


# Agora, podemos iterar pela lista de tuples (**newinfo**) para verificar quais siglas já estão na nossa base de dados, e adicionar aquelas que não estiverem:

# In[15]:


for i in range(len(newinfo)):
    cur.execute("""SELECT nome FROM UF WHERE nome = ? """, (newinfo[i][0], ))
    check = cur.fetchone()[0]
    sigla = newinfo[i][0]
    nome_ext = newinfo[i][1]
    idh = float(newinfo[i][2])/1000
    alfab = float(newinfo[i][3][:-1])    
    if check is None:        
        cur.execute("""INSERT INTO UF (nome, nome_ext, idh, alfabetizacao) VALUES (?, ?, ?, ?)""", (sigla, nome_ext, idh, alfab, ))
        print("Sigla \"" + newinfo[i][0] + "\" não estava presente e foi adicionada à base junto com as demais informações")       
    else:
        cur.execute("""UPDATE UF SET nome_ext = ?, idh = ?, alfabetizacao = ? WHERE nome = ?""", (nome_ext, idh, alfab, sigla,))
        print(sigla, nome_ext, idh, alfab, end = " | ")
print("\n")        
conn.commit()


# <a name="e3.2"></a>
# ### 4.2 Detalhando variáveis já existentes

# Já que estamos adicionando informações em nossa base, vamos aproveitar para **adicionar uma descrição para as siglas na tabela categoria**, que correspondem à intensidade do risco de extinção sob o qual está uma determinada espécie. Do [**Livro Vermelho (P.33, Vol. 1)**](https://www.icmbio.gov.br/portal/images/stories/biodiversidade/fauna-brasileira/livro-vermelho/volumeI/vol_I_parte1.pdf), temos que:  
# RE: Regionalmente Extinta|
# PEX: Provavelmente Extinta| 
# EX: Extinta|
# EW: Extinta na Natureza|
# CR: Criticamente em Perigo|
# EN: Em Perigo|
# VU: Vulnerável|
# DD: Deficiente em Dados 

# In[16]:


cur.executescript("""
ALTER TABLE categoria ADD descricao TEXT;
UPDATE categoria SET descricao = "Vulnerável" WHERE nome = "VU";
UPDATE categoria SET descricao = "Criticamente em Perigo" WHERE nome = "CR";
UPDATE categoria SET descricao = "Em Perigo" where nome = "EN";
UPDATE categoria SET descricao = "Provavelmente Extinta" WHERE nome = "CR(PEX)";
UPDATE categoria SET descricao = "Regionalmente Extinta" WHERE nome = "RE";
UPDATE categoria SET descricao = "Extinta" WHERE nome = "EX";
UPDATE categoria SET descricao = "Provavelmente Extinta na Natureza" WHERE nome = "CR(PEW)";
UPDATE categoria SET descricao = "Extinta na Natureza" WHERE nome = "EW";
""")
print("Coluna adicionada na tabela UC com valores atualizados")


# Podemos fazer o mesmo para as siglas das Unidades de Conservação, utilizando como referência a mesma página do livro vermelho. Contudo, aqui teremos um pouco mais de trabalho, pois temos de **extrair a sigla das strings armazenadas na coluna "nome"** da tabela UC. Primeiro, vamos criar uma nova tabela primária em nossa base de dados para armazenar os tipos de Unidades de Conservação e populá-la. Aproveitaremos para adicionar o campo tipo_id na tabela UC

# In[17]:


cur.executescript("""
ALTER TABLE UC ADD tipo_id INTEGER;
DROP TABLE IF EXISTS tipo_UC;

CREATE TABLE tipo_UC (
    id  INTEGER NOT NULL PRIMARY KEY UNIQUE,
    sigla TEXT UNIQUE,
    nome    TEXT 
);

INSERT INTO tipo_UC (sigla, nome) VALUES ("APA", "Área de Proteção Ambiental");
INSERT INTO tipo_UC (sigla, nome) VALUES ("ARIE", "Área de Relevante Interesse Ecológico");
INSERT INTO tipo_UC (sigla, nome) VALUES ("FLONA", "Floresta Nacional");
INSERT INTO tipo_UC (sigla, nome) VALUES ("FLOE", "Floresta Estadual");
INSERT INTO tipo_UC (sigla, nome) VALUES ("FLOM", "Floresta Municipal");
INSERT INTO tipo_UC (sigla, nome) VALUES ("RESEX", "Reserva Extrativista");
INSERT INTO tipo_UC (sigla, nome) VALUES ("REFA", "Reserva da Fauna");
INSERT INTO tipo_UC (sigla, nome) VALUES ("REDES", "Reserva de Desenvolvimento Sustentável");
INSERT INTO tipo_UC (sigla, nome) VALUES ("RPPN", "Reserva Particular do Patrimônio Natural");
INSERT INTO tipo_UC (sigla, nome) VALUES ("FLOEX", "Floresta Extrativista");
INSERT INTO tipo_UC (sigla, nome) VALUES ("ASPE", "Área de Proteção Integral");
INSERT INTO tipo_UC (sigla, nome) VALUES ("PE", "Parque Estadual");
INSERT INTO tipo_UC (sigla, nome) VALUES ("PM", "Parque Municipal");
INSERT INTO tipo_UC (sigla, nome) VALUES ("PARNA", "Parque Nacional");
INSERT INTO tipo_UC (sigla, nome) VALUES ("FLOREST", "Floresta Estadual");
INSERT INTO tipo_UC (sigla, nome) VALUES ("MN", "Monumento Natural");
INSERT INTO tipo_UC (sigla, nome) VALUES ("REVIS", "Refúgio da Vida Silvestre");
INSERT INTO tipo_UC (sigla, nome) VALUES ("ESEC", "Estação Ecológica");
INSERT INTO tipo_UC (sigla, nome) VALUES ("Parque", "Parque");
INSERT INTO tipo_UC (sigla, nome) VALUES ("RDS", "Reserva de Desenvolvimento Sustentável");
INSERT INTO tipo_UC (sigla, nome) VALUES ("REBIO", "Reserva Biológica");


""")
conn.commit()
print("Feito")


# Agora, vamos iterar por todas as linhas da tabela UC, extrair a sigla da coluna "nome" e utilizá-la para identificar o valor que deve ser adicionado na coluna tipo_id 

# In[18]:


import re
cur.execute("""SELECT count(*) FROM UC """) #retorna o número total de linhas na tabela
rows = cur.fetchone()[0]
count = 0
for i in range(rows):
    cur.execute("""SELECT nome FROM UC """)
    nome = cur.fetchall()[i][0]
    sigla = re.findall("([^ ]+?) .+", nome) #procure, sem ser ganancioso (?), uma sequencia de um ou mais caracteres (+) que não sejam espaços em branco ([^ ]) seguidos por um ou mais caracteres quaisquer (.+). Selecione apenas o que estiver entre parênteses
    cur.execute("""SELECT id FROM tipo_UC WHERE sigla = ?""", (sigla[0], ))
    tipo_id = cur.fetchone()
    if tipo_id is None:
        print("Sigla não encontrada na linha", i)
        continue
    tipo = int (tipo_id[0])
    i = int(i)
    cur.execute("""UPDATE UC SET tipo_id = ? WHERE id = ?""", (tipo, i))
    count += 1 
conn.commit()
print (count, "de", rows, "registros foram atualizados")


# <a name="e3.3"></a>
# ### 4.3 Diagrama completo
# ![diagrama2.png](attachment:diagrama2.png)

# <a name="etapa4"></a>
# ## 5. Contabilizando dados e cruzando informações

# Agora que temos a base de dados completamente populada, podemos realizar consultas filtradas para contabilizar dados e tentar identificar padrões de acordo com as variáveis que temos disponíveis. Seguem algumas possibilidades de análise: 
# 

# <a name="e4.1"></a>
# ### 5.1 Número de espécies ameaçadas por Unidade Federal (UF) e categoria de risco

# Vamos criar uma _query_ que retorna o id da espécie, a categoria de risco em que ela está, a descrição desta categoria, a UF em que ela se encontra e o nome por extenso da UF. Considerando estas variáveis, podemos escrever a _query_ aplicando os unindo as seguintes tabelas (com JOIN) e aplicando os seguintes filtros (com WHERE):
# >**SELECT** especie.id as "ID", categoria.nome as "Status", categoria.descricao as "Descrição", UF.nome as "UF", >UF.nome_ext as "Estado"       
# >**FROM** risco **JOIN** especie **JOIN** categoria **JOIN** UC **JOIN** UF     
# >**WHERE** risco.especie_id = especie.id **AND** risco.categoria_id = categoria.id **AND** risco.UC_id = UC.id **AND**  UC.UF_id = UF.id    
# 
# Em vez de rodar a _query_ e visualizá-la através da extensão SQL do Jupyter, como fiz na seção 3, vou rodá-la como argumento de uma função da biblioteca Pandas que retorna um dataframe com os resultados da consulta. Assim, teremos todos estes dados prontos para uso dentro do ambiente de programação:

# In[19]:


sqlstr = ("""
SELECT especie.id AS "ID", categoria.nome AS "Status", categoria.descricao AS "Descrição", UF.nome AS "UF", UF.nome_ext AS "Estado"  
FROM risco JOIN especie JOIN categoria JOIN UC JOIN UF
WHERE risco.especie_id = especie.id AND risco.categoria_id = categoria.id AND risco.UC_id = UC.id AND  UC.UF_id = UF.id
""")
dados = pd.read_sql_query(sqlstr, conn)
display(dados)


# Vamos criar uma **estrutura de dados** para armazenar informações numéricas sobre cada UF, contabilizando o Total e também as quantidades espécificas para cada "Status". Para isso, criaremos um dicionário **(count_uf)** que contem como chaves as siglas correspondentes de cada UF, onde armazenaremos como valores outros dicionários, que detalham as informações para cada chave no dicionário primário. Teremos algo do tipo:       
# 
# >{          
# 'MG' : {             
# &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'Total' : 100,          
# &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'Vulnerável': 20,      
# &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'Em perigo': 30,         
# &nbsp;&nbsp;&nbsp;&nbsp;&nbsp; .....             
# &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},       
# &nbsp;&nbsp; 'MT' : {           
# &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'Total' : 70,             
# &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'Vulnerável': 10,             
# &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'Em perigo': 20,         
# &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;.....        
# &nbsp;&nbsp;&nbsp;&nbsp;&nbsp; },  
#           
#          
# Primeiro, vamos recuperar uma lista das siglas das UF's utilizando o metodo _pandas.core.frame.DataFrame.unique_ e armazená-la na variável **_ufs_**. Também vamos criar um dicionário chamado **_counts_uf_** para armazenar as informações de cada UF.  Em seguida, iniciamos um loop que itera por todo o elemento **_uf_** da lista **_ufs_**.      
# Aqui, checamos se o valor de **_uf_** não é nulo, criamos um _subset_ do _DataFrame_ que contém apenas as linhas em que a variável **_uf_** corresponde ao valor armazenado na coluna "UF" do _DataFrame_ e contamos o número de linhas, armazenando o resultado na variável **_count_uf_**. Em seguida, checamos se o valor de **_uf_** é, na verdade, uma string com várias siglas separadas por "/".    
# <br>
# Se este for o caso, separamos a string em subunidades utilizando o método _split_ e armazenamos as siglas extraídas como uma lista na variável **_split_**.       
# <br>
# Em seguida, iteramos por todos os elementos da lista **_split_**, verificamos se o elemento (i.e, a sigla) já possui uma chave no dicionário **_counts_uf_** e, caso não tenha, o adicionamos. Feito isso, adicionamos o valor de **_count_uf_** na chave "Total" do dicionário secundário armazenado na chave **_uf_** do dicionário primário (**_counts_uf_**)        
# <br>
# O passo seguinte é fazer a contagem para cada uma das categorias. Iniciamos um sub-loop que itera por toda **_categoria_** da variável **_categorias_**, uma lista que contém todas as strings únicas usadas na coluna Descrição do _DataFrame_. Criamos um subset do dataframe que satisfaz duas condições: a sigla armazenada em **_split\[i\]_** corresponde à sigla armazenada na coluna "UF" do _DataFrame_, e a string armazenada na variável **_categoria_** corresponde à string armazenada na coluna "Descrição" do _DataFrame_. Realizamos uma contagem das linhas que satisfazem essas condições e a adicionamos à chave correspondente no dicionário secundário.         
# <br>
# As etapas seguintes são exatamente as mesmas do que as descritas até agora, porém ajustadas para o caso de não haver múltiplas siglas armazenadas na variável **_uf_**

# In[20]:


ufs = dados["UF"].unique() #recupera as strings únicas presentes na coluna UF do dataframe dados (i.e, as siglas das UFs)
categorias = dados["Descrição"].unique()
counts_uf = dict()

for uf in ufs:
    
    if uf is None: continue #ignore as linhas sem UF    
    subset_uf = dados["UF"] == uf
    count_uf = dados[subset_uf]["UF"].count()       
    if uf.find("/") != -1: #para os casos em que há várias siglas separadas por "/",  
        split = re.split("/", uf) # dividimos a string 
        for i in range(len(split)): 
            
            if split[i] in counts_uf: #checamos se cada uma das siglas já possui um dicionário equivalente em count_uf
                pass
            else: #adicionamos o dicionário, se não existir
                counts_uf[split[i]] = dict()
                
            counts_uf[split[i]]["Total"] = counts_uf[split[i]].get("Total", 0) + count_uf # adicionamos count_uf à chave "Total" para o dicionário de cada uma das siglas extraídas da string          
            
            for categoria in categorias:
                subset_cat = dados[(dados["Descrição"] == categoria) & (dados["UF"] == uf)] 
                count_cat = subset_cat["Descrição"].count()
                counts_uf[split[i]][categoria] = counts_uf[split[i]].get(categoria, 0) + count_cat
    else:
        
        if uf in counts_uf: #análogo aos últimos comentários
            pass
        else:
            counts_uf[uf] = dict()
            
        counts_uf[uf]["Total"] = counts_uf.get("Total", 0) + count_uf
        
        for categoria in categorias:               
                subset_cat = dados[(dados["Descrição"] == categoria) & (dados["UF"] == uf)]
                count_cat = subset_cat["Descrição"].count()
                if categoria is None: categoria = "Não Informado"
                counts_uf[uf][categoria] = counts_uf[uf].get(categoria, 0) + count_cat


# Vamos usar o método _pandas.DataFrame.from_dict_ para transformar este dicionário em um _DataFrame_ pandas e visualizá-lo com o método _display_ que importamos no começo deste caderno:

# In[21]:


colunas = counts_uf["MG"].keys()
totais_uf = pd.DataFrame.from_dict(counts_uf, orient="index", columns = colunas)
display(totais_uf.sort_values(by = ["Total"], ascending=False))


# <a name="e4.2"></a>
# ### 5.2 Número de espécies ameaçadas por índices de IDH e Alfabetização
# Vamos criar um novo dataframe aproveitando a coluna "Total" do dataframe que criamos na etapa anterior e adicionando as informações de IDH e Alfabetização por UF. Para isso, como na última etapa, utilizamos o método pd.read_sql_querry para extrair as informações pertinentes de nossa base de dados na variável **_dados_**. Em seguida, definimos a coluna "UF" como índice do DataFrame dados usando o método set_index(), e então utilizamos o método pd.concat() para juntar a coluna total ao DataFrame dados, armazenando o resultado na variável idh_uf

# In[22]:


sqlstr = ("""
SELECT UF.nome as "UF", UF.idh, uf.alfabetizacao as "Alfabetização" 
FROM UF 
WHERE UF.idh IS NOT NULL AND  UF.alfabetizacao IS NOT NULL
""")
dados = pd.read_sql_query(sqlstr, conn)
dados = dados.set_index("UF")
idh_uf = pd.concat([totais_uf["Total"], dados], axis=1)
display(idh_uf.sort_values(by = ["Alfabetização"], ascending=False))


# <a name="e4.3"></a>
# ### 5.3 Número de espécies ameaçadas por Classe e Divisão
# Aqui, o processo é análogo àquele que seguimos no item 5.1, então não entrarei em detalhes sobre cada procedimento.
# #### Totais por Classe

# In[23]:


sqlstr = ("""
SELECT especie.nome AS "Nome", categoria.nome AS "Status", categoria.descricao AS "Descrição", classe.nome AS "Classe", divisao.nome AS "Divisão"  
FROM risco JOIN especie JOIN categoria JOIN UC JOIN classe JOIN divisao
WHERE risco.especie_id = especie.id AND risco.categoria_id = categoria.id AND risco.UC_id = UC.id AND  especie.classe_id = classe.id AND especie.divisao_id = divisao.id
""")
dados = pd.read_sql_query(sqlstr, conn)
classes = dados["Classe"].unique()
categorias = dados["Descrição"].unique()
counts_classe = dict()

for classe in classes:
    
    if classe is None: continue #ignore as linhas sem classe    
    subset_classe = dados["Classe"] == classe
    count_classe = dados[subset_classe]["Classe"].count()
    if classe in counts_classe: pass
    else:counts_classe[classe] = dict()    
    counts_classe[classe]["Total"] = counts_classe.get("Total", 0) + count_classe
    for categoria in categorias:               
        subset_cat = dados[(dados["Descrição"] == categoria) & (dados["Classe"] == classe)]
        count_cat = subset_cat["Descrição"].count()
        if categoria is None: categoria = "Não Informado"
        counts_classe[classe][categoria] = counts_classe[classe].get(categoria, 0) + count_cat
        
colunas = counts_classe["Mammalia"].keys()
totais_classe = pd.DataFrame.from_dict(counts_classe, orient="index", columns = colunas)
display(totais_classe)


# #### Totais por Divisão
# Nota: poderiamos ter abstraído este procedimento em uma função. Contudo, como irei utilizá-lo poucas vezes, optei por repetir o código:

# In[24]:


divisoes = dados["Divisão"].unique()
counts_divisao = dict()
for divisao in divisoes: 
    if divisao is None: continue #ignore as linhas sem classe    
    subset_divisao = dados["Divisão"] == divisao
    count_divisao = dados[subset_divisao]["Divisão"].count()
    if divisao in counts_divisao: pass
    else:counts_divisao[divisao] = dict()    
    counts_divisao[divisao]["Total"] = counts_divisao.get("Total", 0) + count_divisao
    for categoria in categorias:               
        subset_cat = dados[(dados["Descrição"] == categoria) & (dados["Divisão"] == divisao)]
        count_cat = subset_cat["Descrição"].count()
        if categoria is None: categoria = "Não Informado"
        counts_divisao[divisao][categoria] = counts_divisao[divisao].get(categoria, 0) + count_cat
        
colunas = counts_divisao["Anfíbios"].keys()
totais_divisao = pd.DataFrame.from_dict(counts_divisao, orient="index", columns = colunas)
display(totais_divisao)


# <a name="e4.4"></a>
# ### 5.4 Número de espécies ameaçadas por tipo de Unidade de Conservação
# Aqui, também seguimos um processo análogo ao dos demais itens:

# In[25]:


sqlstr = ("""
SELECT especie.nome AS "Nome", categoria.nome AS "Status", categoria.descricao AS "Descrição", UC.nome as "UC", tipo_UC.nome as "Tipo de UC"  
FROM risco JOIN especie JOIN categoria JOIN UC JOIN tipo_UC
WHERE risco.especie_id = especie.id AND risco.categoria_id = categoria.id AND risco.UC_id = UC.id AND  UC.tipo_id = tipo_UC.id
""")
dados = pd.read_sql_query(sqlstr, conn)
tipo_ucs = dados["Tipo de UC"].unique()
categorias = dados["Descrição"].unique()
counts_tipo_uc = dict()

for tipo in tipo_ucs:
    
    if tipo is None: continue #ignore as linhas sem tipo uc    
    subset_tipo = dados["Tipo de UC"] == tipo
    count_tipo = dados[subset_tipo]["Tipo de UC"].count()
    if tipo in counts_tipo_uc: pass
    else:counts_tipo_uc[tipo] = dict()    
    counts_tipo_uc[tipo]["Total"] = counts_tipo_uc.get("Total", 0) + count_tipo
    for categoria in categorias:               
        subset_cat = dados[(dados["Descrição"] == categoria) & (dados["Tipo de UC"] == tipo)]
        count_cat = subset_cat["Descrição"].count()
        if categoria is None: categoria = "Não Informado"
        counts_tipo_uc[tipo][categoria] = counts_tipo_uc[tipo].get(categoria, 0) + count_cat
        
colunas = counts_tipo_uc["Parque Estadual"].keys()
totais_tipo_uc = pd.DataFrame.from_dict(counts_tipo_uc, orient="index", columns = colunas)
display(totais_tipo_uc)


# <a name="etapa5"></a>
# ## 6. Visualizações e gráficos
# Consulte a seção de "links úteis" da bilbiografia para acessar a documentação do metodo _plot_

# <a name="e5.1"></a>
# ### Total de Espécies Ameaçadas por UF e Grau de Risco

# In[26]:


import matplotlib.pyplot as plt
totais_uf["Total"].plot(kind="bar", title="Total de Espécies Ameaçadas por UF", figsize=(16, 10))
totais_stack = totais_uf.drop(["Total"], axis = 1)
totais_stack.plot.bar(title="Espécies Ameaçadas por UF e Risco", stacked=True, figsize=(16, 10));


# In[27]:


#totais_uf.plot(kind="bar", subplots=True, figsize=(18, 18))
#plt.delaxes


# <a name="e5.2"></a>
# ### Dispersão entre Número de Espécies em Risco e IDH ou Alfabetização 

# In[28]:


fig, axes = plt.subplots(nrows=1,ncols=2,figsize=(16,6)) #criando a grade 1x2 para organizar os gráficos
idh_uf.plot.scatter(ax = axes [0], x='idh', y='Total', title= "Dispersão entre IDH e número de casos de risco");
idh_uf.plot.scatter(ax = axes [1], x='Alfabetização', y='Total', title= "Dispersão entre alfabetização e número de casos de risco");
plt.show()


# Vemos que evidentemente **não há nenhuma relação** entre os casos de risco e a alfabetização. Embora os pontos estejam levemente mais concentrados no gráfico de casos de risco por idh, também não podemos concluir que há uma relação linear entre estas duas variáveis. Contudo, para fins de aprendizado, vamos traçar a linha de regressão para a relação "Total" e "idh" utilizando a biblioteca _seaborn_

# In[29]:


import seaborn as sb
fig, axes = plt.subplots(nrows=1,ncols=1,figsize=(7.2,6)) #criando a grade para definir o tamanho do gráfico
sb.regplot(ax = axes, x = "idh", y ="Total", ci = None, data=idh_uf)
plt.show()


# <a name="e5.3"></a>
# ### Distribuição de Espécies ameaçadas por divisão e Grau de Risco

# In[32]:


totais_divisao.plot.pie(y="Total", figsize = (14, 14), legend = False, autopct='%1.1f%%', title = "Distribuição de Espécies Ameaçadas por Divisão")
plt.axis('off')
divisao_stack = totais_divisao.drop(["Total"], axis = 1)
divisao_stack.plot.bar(title="Número de Espécies Ameaçadas por Divisão e Risco", stacked=True, figsize=(16, 8));


# <a name="e5.4"></a>
# ### Distribuição e Número de Espécies Ameaçadas por tipo de Unidade de Conservação

# In[36]:


totais_tipo_uc.plot.pie(y="Total", figsize = (14, 14), legend = False, autopct='%1.1f%%', title = "Distribuição de Espécies Ameaçadas por tipo de UC")
plt.axis('off')
uc_stack = totais_tipo_uc.drop(["Total"], axis = 1)
uc_stack.plot.bar(title="Espécies Ameaçadas por UF e Risco", stacked=True, figsize=(16, 10));


# <a name="biblio"></a>
# ## Bibliografia:
# 
# CNCFlora (2016). _Espécies Ameaçadas da Flora em UCs Federais_, versão 2016-10-03. Disponível em:http://ckan.jbrj.gov.br/dataset/ameacadas-em-ucs-federais. Acesso em 30/07/2021 (data de download)     
# <br>
# Instituto Chico Mendes de Conservação da Biodiversidade (2008). _Livro Vermelho da Fauna Brasileira Ameaçada de Extinção_, 
# Vol.1. Disponível em: https://www.icmbio.gov.br/portal/images/stories/biodiversidade/fauna-brasileira/livro-vermelho/volumeI/vol_I_parte1.pdf. Acesso em 02/07/2021
# 

# #### Links Úteis:

# [Pandas Intro Tutorials](https://pandas.pydata.org/pandas-docs/stable/getting_started/intro_tutorials/index.html)  
# [10 Minutes to Pandas](https://pandas.pydata.org/pandas-docs/stable/user_guide/10min.html)        
# [Pandas API Reference](https://pandas.pydata.org/pandas-docs/stable/reference/index.html)      
# [Pandas Chart Visualization](https://pandas.pydata.org/pandas-docs/stable/user_guide/visualization.html#visualization-barplot)       
# [Stack Overflow](https://stackoverflow.com/)     
# [GeeksforGeeks](https://www.geeksforgeeks.org/)       
# [Portal Brasileiro de Dados Abertos](https://dados.gov.br/)
