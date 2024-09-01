# dbt_duckdb
Treinamento de DBT com DuckDB. Introdução objetiva para você aprender na prática.

Bem-vindo! Vamos aprender sobre DBT (Data Build Tool) utilizando DuckDB. Este repositório contém alguns materiais úteis, exemplos e exercícios para ajudá-lo a aprender e dominar o uso do DBT com o DuckDB.

## O que é DBT?

DBT (Data Build Tool) é uma ferramenta de linha de comando open-source que auxilia analistas e engenheiros de dados a transformarem dados em seus data warehouses de maneira mais eficaz. Ele permite que você escreva consultas SQL modulares, teste suas transformações de dados e documente seus modelos de dados. O DBT foca na camada de transformação do processo ELT (Extract, Load, Transform), facilitando a construção e manutenção de pipelines de dados complexos.

## O que é DuckDB?

DuckDB é um sistema de gerenciamento de banco de dados OLAP SQL embutido. Ele é projetado para suportar cargas de trabalho de consultas analíticas de forma eficiente e pode ser incorporado em outras aplicações. O DuckDB é conhecido por seu alto desempenho, facilidade de uso e capacidade de lidar com grandes conjuntos de dados em uma única máquina.


## Escopo
- Instalação do DBT em ambiente virtual habilitado para duckdb
- Ingestão da base de dados
- Criação do projeto dbt
- Configuração de profile
- Configuração de dbt_project
- Executar primeiro modelo
- Configuração de source
- Configuração do dbt_project
- Criação de modelo SQL
	- Com fonte no BD
	- Com fonte em outro modelo
- Criação de modelo python
- Declaração de metadados de modelo
- Gerar e visualizar documentação
	- Lineage: Visualização gráfica dos modelos
- Testes de modelo
	- Exemplo de teste genérico (unique)
- Jinja - Criação de SQL via template
  - Compilação de modelos
- Macros - Funções reutilizáveis com parâmetros
- Comandos frequentes
- Materiais de suporte
- Alguns tópicos relevantes, mas não contemplados:
	-  modelos incrementais, slow changing dimensions com snapshots, testes singulares em SQL

## Estrutura do Repositório

Este repositório está organizado da seguinte forma:

- `../.venv`: Ambiente virtual
- `./`: Pasta raiz do repositório chamada dbt_duckdb.
- `./ingestao/`: Contém o script para criar e ingerir os dados brutos no banco de dados.
- `./ingestao/data/`: Onde deve ser salvo os arquivos de dados brutos para ingestão.
- `./query/`: Contém o notebook para consulta de dados no banco de dados.
- `./db/`: Onde será gerado o banco de dados duckdb, que é apenas um arquivo.

Estrutura básica de um projeto dbt:
- `./duck/`: O projeto DBT em si
- `./duck/dbt_project.yml`: Arquivo de configuração do projeto DBT.
- `./duck/models/`: Contém os modelos DBT, que são arquivos SQL que definem as transformações.
- `./duck/macros/`: Macros DBT personalizadas para estender a funcionalidade do DBT. (Não contemplado)
- `./duck/tests/`: Casos de teste para garantir a correção das transformações de dados.

## Começando

Para começar, você precisa ter o [Python](https://www.python.org/downloads/) e o [Git](https://git-scm.com/downloads) instalados em sua máquina.

Além disso, você deve ter um visualizador de notebooks instalado em sua máquina. Como o [Jupyter Lab](https://jupyter.org/install) ou o [Visual Studio Code](https://code.visualstudio.com/download) com a extensão python.

Este treinamento foi desenvolvido com comandos de terminal compatíveis com o sistema operacional **Windows**, mas pode ser facilmente adaptado para **Linux** e **Mac**.

**Clone o repositório**

Abra um terminal em uma pasta vazia e execute os comandos abaixo:
```bash
git clone https://github.com/derycck/dbt_duckdb
cd dbt_duckdb
```

## Instalação do DBT

Abra o terminal dentro da pasta do repositório clonado. Agora vamos criar um ambiente virtual com as instalações necessárias para usar o DBT com duckdb e uma lib para manipulação de dados tabulares.

Para isso, execute o script `_install.bat`. Além desse script instalar tudo, ele também cria scripts de atualização do ambiente virtual e de ativação do ambiente virtual. Eles serão gerados em:

- `../activate_venv_update.bat`: Atualização do ambiente virtual
- `../activate_venv.bat`: Ativação do ambiente virtual

Se preferir instalar manualmente, digite os comandos abaixo no terminal aberto na pasta do repositório.
```bash
python -m venv ..\.venv
..\.venv\Scripts\activate # No linux ou mac use `source venv/bin/activate`
python -m pip install --upgrade pip
pip install dbt-duckdb pandas
```

Caso feche o terminal, para ativar o ambiente virtual novamente, execute o script `../activate_venv.bat` ou abra o terminal na pasta do repositório e digite `..\.venv\Scripts\activate`.

Com isso, a pasta do ambiente virtual chamada ".venv" ficará localizada ao lado da pasta do repositório.

## Ingestão da base de dados

- Baixe as duas bases de dados e salve em `./ingestao/data`
  - [Sample_Fact_Top_Material.zip](https://github.com/andrezaleite/PES_Embraer_DBT/raw/main/data/Sample_Fact_Top_Material.zip)
  - [Sample_Gestao_Faltas.zip](https://github.com/andrezaleite/PES_Embraer_DBT/raw/main/data/Sample_Gestao_Faltas.zip)

Para criar o banco de dados:
- Abra um terminal com o ambiente virtual ativado na pasta do repositório
- Execute o arquivo de ingestão com o comando abaixo:

```shell
python ingestao/ingestao.py
```

Repare que o banco de dados será criado em `db/dev.duckdb`.

Se quiser entender detalhes sobre cada etapa da ingestão, abra o notebook `ingestao/ingestao.ipynb` no VSCODE ou outro visualizador.

## Criação do projeto DBT

Vamos criar um projeto DBT chamado `duck` para seguir com esse treinamento.

Se certifique de estar com o terminal aberto na pasta do repositório e com o ambiente virtual ativado. Execute o comando abaixo e siga as instruções no terminal
```shell
dbt init
```

As instruções podem com o passar das versões do DBT, mas essencialmente são:
- Digitar um nome para o projeto. Digite `duck`.
- Escolher o banco de dados para a configuração de profile. Como o DBT foi instalado usando a extensão "dbt-duckdb", aparecerá apenas a opção do banco de dados duckdb. Assim, digite "1" e prossiga

## Configuração de profile

em `duck/profile.yml`
```yml
duck:
  outputs:
    dev:
      type: duckdb
      path: "../db/dev.duckdb"
      threads: 1
      schema: dbt

  target: dev
```


## Executar primeiro modelo

Primeiro vamos acessar a pasta do projeto pelo terminal. Estando com o terminal com o ambiente virtual ativado, adepois digite:
```shell
cd duck
```

```shell
dbt run -s my_first_dbt_model
```

Confira se a nova tabela foi criada em `query/query.ipynb` ou executando o script abaixo:
```python
import duckdb
from pathlib import Path

def run_select_sql(query_str):
    with duckdb.connect(str(duckdb_file)) as con:
        result_ = con.execute(query_str).fetchdf()
    return result_

duckdb_file = Path('.').absolute().parent/'db'/"dev.duckdb"
assert duckdb_file.exists()


query_str = "SHOW ALL TABLES"
tables = run_select_sql(query_str)
print(tables)
```
## Configuração de source
Crie o arquivo `_SOURCES.yml` na pasta `models` e preencha com:

`duck/models/_SOURCES.yml`
```yml
version: 2

sources:
  - name: raw
    description: schema raw
    tables:
      - name: FACT_TOP_MATERIAL
      - name: GESTAO_FALTAS
      - name: MATERIAL_PIVOT
```
## Configuração de dbt_project

Vamos declarar que o tipo de declaração dos modelos serão determinados pela pasta onde eles se encontram.

Faça uma pequena edição ao final do arquivo, indicando que:
- os modelos criados na pasta models/staging , devem ser materializados como tabela no schema **staging**.
- os modelos criados na pasta models/serving , devem ser materializados como tabela no schema **serving**.

```yml
models:
  duck:
    example:
      +materialized: view
    staging:
      +materialized: table
      +schema: staging
    serving:
      +materialized: table
      +schema: serving
```

## Criação de modelo com fonte no BD
Durante a construção de um sql model, para fazer referência a um modelo no banco de dados, usamos notação jinja, com a função source. Onde o primeiro argumento deve ser o schema e o segundo argumento deve ser o nome da coluna.

Para exemplificar, crie o modelo abaixo:

`duck/models/staging/dim_material.sql`
```sql
with source_data as (
    select *
    from
    {{ source('raw', 'FACT_TOP_MATERIAL') }}

)

select *
from source_data
```
Execute com:
```shell
dbt run -s dim_material
```

Confira se a nova tabela reaproveitando usando o notebook de query ou com o script informado anteriormente, mas editando para a query abaixo:
```sql
select * from dbt_staging.dim_material
```

Agora vamos criar um modelo que use mais recursos do SQL.

A documentação do DBT recomenda adotar como boa prática, o uso de CTEs em substituição ao uso de subqueries.

Com CTEs, primeiro deve-se selecionar as colunas de cada fonte de dados, para ao final fazer a query final.

Essa boa prática busca facilitar a manutenabilidade das queries de transformação, uma vez que é preciso menos esforço mental seguindo esse estilo para interpretar as queries e identificar origem de cada coluna utilizada na transformação.

Crie o modelo abaixo:

`duck/models/staging/dim_preco.sql`
```sql
with
    ftm as (
        SELECT DISTINCT
            FTOP_PART_NUMBER
            , FTOP_USD_PRECO_UNIT
        FROM
            {{source("raw", "FACT_TOP_MATERIAL")}}
),
    gf as (
        SELECT DISTINCT
            PROJETO
            , PN_MAT_FALT
        FROM
            {{source("raw","GESTAO_FALTAS")}}
)

select
    gf.PROJETO
    , gf.PN_MAT_FALT
    , ftm.FTOP_USD_PRECO_UNIT
from gf
join ftm
    on gf.PN_MAT_FALT = ftm.FTOP_PART_NUMBER
```

Execute com:
```shell
dbt run -s dim_preco
```

Consulte a nova tabela reaproveitando usando o notebook de query ou com o script informado anteriormente, mas editando para a query abaixo:
```sql
select * from dbt_staging.dim_preco
```

## Criação de modelo com fonte em outro modelo
Para fazer referência a um modelo existente no DBT, usamos notação jinja com a função REF. O nome de modelo deve ser único em todo o projeto, assim não é necessário informar schema.

Apesar de um modelo DBT após ser materializado, possa ser referenciado através da função Source, existe algumas vantagens diferenciais em utilizar a função REF.
Dentre as principais, é possível citar a criação de uma relação de dependência explícita entre os modelos, o que permite ao DBT gerenciar a ordem de execução dos modelos automaticamente. Outra vantagem é a exposição gráfica dessa relação de dependência entre os modelos através do gráfico Lineage.

Crie o arquivo `duck/models/serving/dim_projeto_stats.sql`
```sql
select
    PROJETO
    , count(PN_MAT_FALT) as "COUNT_PN_MAT_FALT"
    , avg(FTOP_USD_PRECO_UNIT) as "AVG_FTOP_USD_PRECO_UNIT"
from {{ref("dim_preco")}}
group by *
order by "PROJETO"
```
Execute com:
```shell
dbt run -s dim_projeto_stats
```

Consulte a nova tabela reaproveitando usando o notebook de query ou com o script informado anteriormente, mas editando para a query abaixo:
```sql
select * from dbt_serving.dim_projeto_stats
```

## Criação de modelo python

Em certos momentos é necessário realizar transformações de dados que não podem ser feitas apenas com SQL.
Nesse caso, o DBT permite a criação de modelos Python, que são scripts Python que podem ser executados no fluxo de trabalho do DBT. Isso permite que você use a linguagem Python para realizar transformações de dados complexas, integrar com quaisquer bibliotecas e executar qualquer lógica que não possa ser expressa em SQL.

Desse modo, modelos python sustentam análise de dados avançadas, o que amplia significativamente as possibilidades de manipulação e análise de dados dentro do DBT com DuckDB.

Primeiramente vamos criar um modelo python a partir de uma tabela de source. Repare que a função `dbt.source` é utilizada para referenciar a tabela.

Crie o arquivo `duck/models/staging/dim_material_py.py`
```python
import pandas as pd

def model(dbt, session):
    df_fact_top_material: pd.DataFrame = dbt.source('raw', 'FACT_TOP_MATERIAL')

    return df_fact_top_material
```

O modelo python sempre deve contem uma função chamada `model`, que recebe dois argumentos: `dbt` e `session`. O argumento `dbt` é um objeto que contém métodos para referenciar modelos e tabelas, enquanto o argumento `session` é um objeto que contém informações sobre a sessão atual do DBT. Essa função deve sempre retornar um DataFrame pandas.

Uma grande vantagem é que, como a tabela é carregada como um DataFrame pandas, você pode realizar qualquer operação de manipulação de dados que o pandas permitir. Além disso, caso não seja suficiente, é possível importar outras bibliotecas Python e executar operações mais complexas.

Por fim vamos criar um modelo python, mas dessa vez a partir de um outro modelo do DBT. Repare que a função `dbt.ref` é utilizada para referenciar o modelo.

Crie o arquivo `duck/models/serving/dim_projeto_stats_py.py`

```python
import pandas as pd

def model(dbt, session):
    df_dim_preco: pd.DataFrame = dbt.ref('dim_preco').fetchdf()

    df_result = (
      df_dim_preco.groupby('PROJETO')
      .agg(COUNT_PN_MAT_FALT=pd.NamedAgg(column='PN_MAT_FALT', aggfunc='count'),
           AVG_FTOP_USD_PRECO_UNIT=pd.NamedAgg(column='FTOP_USD_PRECO_UNIT',
           aggfunc='mean'))
      .reset_index()
      .sort_values(by='PROJETO')
    )

    return df_result
```

## Declaração de metadados de modelo

Os modelos são documentados através de arquivos YML, que também são usados para escrever declarar testes. Estes arquivos sustentam o microblog de documentação autogerado pelo DBT.
Apesar de ser possível criar um único arquivo yml para documentar todos os modelos, isso dificulta a manutenção do projeto na medida em que ele cresce.

Uma boa prática recomendada pela documentação oficial do DBT, é criar um arquivo de metadados por modelo. Preferencialmente com o prefixo `_model_`.

Crie o arquivo de metadados yml do modelo dim_preco:

`duck/models/staging/_model_preco.yml`
```yml
version: 2

models:
  - name: dim_preco
    description: "Esta tabela contem dados de preco"
    columns:
      - name: PROJETO
        data_type: integer
        description: "Modelo do Produto"
      - name: PN_MAT_FALT
        data_type: string
        description: 'Part Number do item faltante'
      - name: FTOP_USD_PRECO_UNIT
        data_type: float
        description: "Preço máximo do item em unidade de movimentação"
```


Crie o arquivo de metadados yml do modelo dim_projeto_stats:
`duck/models/serving/_model_projeto_stats.yml`
```yml
version: 2

models:
  - name: dim_projeto_stats
    description: "Esta tabela contem média de preço e contagem de materiais por projeto"
    columns:
      - name: PROJETO
        data_type: integer
        description: "Modelo do Produto"
      - name: COUNT_PN_MAT_FALT
        data_type: string
        description: 'Contagem de materiais faltantes'
      - name: FTOP_USD_PRECO_UNIT
        data_type: float
        description: "Preço médio dos ítens do projeto"
```

## Gerar e visualizar documentação

Execute os comandos abaixo:
```shell
dbt docs generate
dbt docs serve
```
Com o segundo comando, o microblog da documentação é aberto, onde o botão flutuante no canto inferior direito abre o gráfico Lineage onde se pode visualizar a relação de dependência entre todos os modelos do projeto.

## Testes de modelo

Aplicaremos como exemplo um teste genérico de valores únicos em uma coluna.
Edite o arquivo  de metadados do modelo "dim_projeto_stats" adicionando as últimas duas linhas sobre `data_tests` informadas abaixo:

`duck/models/serving/_model_projeto_stats`
```yml
      - name: COUNT_PN_MAT_FALT
        data_type: string
        description: 'Contagem de materiais faltantes'
        data_tests:
          - unique
```

Execute o comando:
```shell
dbt test -s dim_projeto_stats
```

Com isso no terminal será gerado um completo relatório de testes, informando inclusive que a coluna com o teste de valores únicos teve o teste falho.

Os testes genéricos contemplam `unique, not_null, accepted_values e relationships` , todos bem detalhados na documentação.

Além disso, é possível escrever testes singulares, baseado em queries SQL.

Para realizar um teste seguido da execução de modelo, o comando abaixo pode ser utilizado.
```shell
dbt build -s dim_projeto_stats
```
E para testar todos os modelos de nosso projeto:
```shell
dbt test
```

Também é possível testar todos os modelos pertencentes a uma determinada Tag, mas esse será assunto para a próxima versão do treinamento.

## Jinja - Criação de SQL via template
Jinja é uma linguagem de modelagem de texto de forma programática. No DBT ela pode ser usada para gerar modelos SQL a partir de um SQL template.

Ao usar Jinja no DBT, você pode definir variáveis, lógica condicional, estruturas de repetição para que após compiladas, produzam um modelo SQL final. Como resultado, o modelo SQL escrito se torna simples e curto, facilitando a manutenção e a reutilização de código.

Com Jinja, também é possível criar macros, que são blocos de código reutilizáveis que podem ser chamados em qualquer parte do projeto. Mas neste tópico abordaremos o uso do Jinja apenas para criação de modelo SQL.

Vamos criar um modelo SQL que utiliza Jinja para realizar a união de 12 tabelas. Se fossêmos criar um SQL puro, seria necessário escrever 12 blocos de código SQL, um para cada tabela, e depois unir todos os blocos com a cláusula UNION ALL, o que custaria pelo menos 48 linhas. Com Jinja, tudo poderá ser escrito em 10 linhas.

Primeiramente devemos informar ao DBT quais novas tabelas do banco de dados devem ser tratadas como fonte de dados. para isso, edite o arquivo de fonte de dados para incluir as novas tabelas que utilizaremos no exemplo. Adicione ao final do arquivo:

`duck/models/_SOURCE.yml`
```yml
      - name: GESTAO_FALTAS_2022_01
      - name: GESTAO_FALTAS_2022_02
      - name: GESTAO_FALTAS_2022_03
      - name: GESTAO_FALTAS_2022_04
      - name: GESTAO_FALTAS_2022_05
      - name: GESTAO_FALTAS_2022_06
      - name: GESTAO_FALTAS_2022_07
      - name: GESTAO_FALTAS_2022_08
      - name: GESTAO_FALTAS_2022_09
      - name: GESTAO_FALTAS_2022_10
      - name: GESTAO_FALTAS_2022_11
      - name: GESTAO_FALTAS_2022_12
```

Note que no banco de dados, o nome das tabelas segue um padrão, onde o nome da tabela é "GESTAO_FALTAS_2022_" seguido do mês. Por esta razão vamos utilizar Jinja para gerar gerar uma lista de meses e fazer a união de todas as tabelas através de uma estrutura de repetição.

Crie o modelo SQL abaixo.

`duck/models/staging/por_mes/dim_gestao_falta_2022.sql`
```sql
-- ["01", "02", ... "12"]
{% set months = range(1, 12+1) %}
{% for month in months %}
    select
        *
    from
        {{ source('raw', 'GESTAO_FALTAS_2022_' + '{:02d}'.format(month)) }}
    {% if not loop.last %}
    union all
    {% endif %}
{% endfor %}
```

Ao usar Jinja para gerar um código SQL, a primeira coisa que devemos nos perguntar é se o código criado após ser compilado vai gerar um código SQL válido. Para isso, é possível compilar o modelo SQL com o comando `dbt compile`.

Entre no terminal com o comando abaixo:

```bash
dbt compile -s dim_gestao_falta_2022
```

O resultado no terminal será o modelo SQL compilado, podendo ser inspecionado visualmente para saber se atinge suas expectativas antes mesmo de executar o modelo contra o banco de dados.

O terminal também pode informar erro de sintaxe no código Jinja que impede a compilação do modelo SQL, informando a linha em que o erro ocorre, o que auxilia no processo de correção do código.

Neste modelo SQl que concatena 12 tabelas, seria necessário 4 linhas para cada mês, resultando em um arquivo SQL com 48 linhas. Mas com auxílio da notação Jinja, o modelo SQL criado tem apenas 10 linhas, o que facilita a manutenção e a leitura do código.

Apesar do modelo SQL ter escrito em poucas linhas, quando o modelo for executado por trás das cortinas o código Jinja é compilado e o SQL puro de 48 linhas que será executado no banco de dados.

Após compilado, caso o código pareça um SQL válido e de acordo com suas expectativas, o modelo SQL já pode ser executado contra o banco de dados. Utilize o comando abaixo:

```bash
dbt run -s dim_gestao_falta_2022
```

Repare que apesar do modelo SQL está dentro de duas pastas (`staging/por_mes`) , ele é executado normalmente pelo seu nome. Isso ocorre porque o DBT exige que cada modelo SQL tenha um nome único, desse modo ele pode ser invocado apenas pelo seu nome independente local onde o arquivo esteja.

As pastas são usadas apenas como uma forma de organizar os modelos e para possibilitar definições ou execuções para o conjunto de modelos dentro da pasta. Definições como tipo de materialização, schema a ser salvo no banco de dados, através do arquivo `dbt_project.yml`...  E execuções como testes, compilação, run e build de modelos.

## Macros - Funções reutilizáveis com parâmetros

Macros são blocos de código reutilizáveis que podem ser chamados em qualquer parte do projeto. Eles são úteis para evitar a repetição de código. Com macros, você pode definir uma função que aceita parâmetros e retorna um bloco de código SQL.

Elas também são escritas em notação Jinja, mas arquivadas em pastas separadas dos modelos, em `macros/`.

Escopo da macro: Unpivot
Input: tabela do banco de dados no schema raw

Vamos explorar a tabela "Material_PIVOT"  que possui as colunas `"DESCR_MAT_FALT, BOT1, EUG1, GPX1, SJK1"`. As 4 últimas colunas correspondem a quantidade de materiais.

Nosso objetivo é transformar esses dados criando uma nova tabela com apenas 3 colunas: `"DESCR_MAT_FALT, FORNECEDOR, COUNT"`. Onde a coluna "FORNECEDOR" contém os valores `"BOT1", "EUG1", "GPX1" e "SJK1"` e a coluna "COUNT" contém os valores correspondentes a cada fornecedor.

Essa operação é conhecida como unpivot, e é uma operação comum em análise de dados.

Primeiramente devemos informar ao DBT quais novas tabelas do banco de dados devem ser tratadas como fonte de dados. para isso, edite o arquivo de fonte de dados para incluir a nova tabela que utilizaremos no exemplo. Adicione ao final do arquivo:

`duck/models/_SOURCE.yml`
```yml
      - name: MATERIAL_PIVOT
```

Agora vamos criar um SQL puro que realiza essa operação. Em seguida, vamos criar uma macro que realiza a mesma operação, mas de forma reutilizável.

Crie o modelo SQL abaixo:

`duck/models/staging/material_unpivot_sqlpuro.sql`
```sql
with material_pivot as (
    select
        DESCR_MAT_FALT
        , BOT1
        , EUG1
        , GPX1
        , SJK1
    from
        {{ source('raw', 'MATERIAL_PIVOT') }}
)

select
    DESCR_MAT_FALT
    , 'BOT1' as "FORNECEDOR"
    , BOT1 as "COUNT"
from
    material_pivot
union all
select
    DESCR_MAT_FALT
    , 'EUG1' as "FORNECEDOR"
    , EUG1 as "COUNT"
from
    material_pivot
union all
select
    DESCR_MAT_FALT
    , 'GPX1' as "FORNECEDOR"
    , GPX1 as "COUNT"
from
    material_pivot
union all
select
    DESCR_MAT_FALT
    , 'SJK1' as "FORNECEDOR"
    , SJK1 as "COUNT"
from
    material_pivot
```

Execute o modelo criado com:
```shell
dbt run -s material_unpivot_sqlpuro
```

Podemos observar que o código acima é repetitivo e possui uma estrutura previsível.

Em um primeiro momento podemos cogitar o uso de código Jinja encurtar o código através de uma estrutura de repetição. Mas como este tipo de processamento de dados é comum e pode ser necessário em outros modelos, a melhor abordagem é criar uma macro que possa realizar essa operação em outros modelos de forma similar a uma chamada de função.

Vamos criar a macro `unpivot` que realiza essa operação a partir de parâmetros.

Crie a macro abaixo:

`duck/macros/utils.sql`
```sql
{% macro unpivot(table_name, columns_to_select, category_column, value_column, columns_to_unpivot) %}
{%- for column in columns_to_unpivot -%}
select
    {{ columns_to_select | join(', ') }}
    , '{{ column }}' as "{{ category_column }}"
    , {{ column }} as "{{ value_column }}"
from
    {{ table_name }}
{% if not loop.last %}
union all
{% endif %}
{%- endfor -%}
{% endmacro %}
```

Em seguida, vamos utilizar a macro `unpivot` para criar um modelo SQL que realiza a operação.
Crie o modelo sql abaixo:

`duck/models/staging/material_unpivot.sql`
```sql
with material_pivot as (
    select
        DESCR_MAT_FALT
        , BOT1
        , EUG1
        , GPX1
        , SJK1
    from
        {{ source('raw', 'MATERIAL_PIVOT') }}
)

{%- set table_name = 'material_pivot' %}
{%- set columns_to_select = ['DESCR_MAT_FALT'] %}
{%- set category_column = 'FORNECEDOR' %}
{%- set value_column = 'COUNT' %}
{%- set columns_to_unpivot = ['BOT1', 'EUG1', 'GPX1', 'SJK1'] %}
{{ unpivot(table_name, columns_to_select, category_column, value_column, columns_to_unpivot) }}
```

Execute o modelo criado com:
```shell
dbt run -s material_unpivot
```

Ao consultar a tabela 'material_unpivot' com 'material_unpivot_sqlpuro' no banco de dados, será possível observar que os resultados são idênticos.

Se o objetivo fosse realizar a mesma operação em uma tabela com dezenas de colunas, construir em um modelo SQL puro seria resultaria em um código extenso e repetitivo com talvez centenas de linhas.

Mas com o auxilio da macro criada, a operação é realizada sempre com a mesma quantidade de linhas, mudando apenas as variáveis que são passadas como parâmetros.



## Comandos frequentes

```bash
// inicia novo projeto
dbt init

// executa um modelo específico
dbt run -s nome_do_modelo

// testa um modelo
dbt test -s nome_do_modelo

// gerar documentação. Precisa estar conectado ao BD
dbt docs generate

// abrir servidor da documentação
dbt docs serve

// Aplica run e test em um modelo
dbt build -s nome_do_modelo

// aplica run e test sequencialmente pra um modelo e seus ascendentes (upstreams)
dbt build -s +nome_do_modelo

// compila modelo. Útil para converter sqlmodel com jinja em sql compilado
dbt compile -s nome_do_modelo
```

Materiais de suporte:

- adaptador duckdb - [duckdb-setup](https://docs.getdbt.com/docs/core/connect-data-platform/duckdb-setup)
- configuracao de source - [sources](https://docs.getdbt.com/docs/build/sources)
- propriedades de modelo - [model-properties](https://docs.getdbt.com/reference/model-properties)
- testes - [data-tests](https://docs.getdbt.com/docs/build/data-tests)
- jinja e macro - [jinja-macros](https://docs.getdbt.com/docs/build/jinja-macros)
- configuração do [dbt_project](https://docs.getdbt.com/reference/dbt_project.yml)
- tipos de materializações - [materializations](https://docs.getdbt.com/docs/build/materializations)
- duckdb - [meta queries](https://duckdb.org/docs/guides/meta/list_tables.html)
- DBT python model no adaptador duckdb: [duckdb python reference](https://duckdb.org/docs/api/python/reference/)
- DBT-Curso introdutório oficial e gratuito - [courses-getdbt](https://courses.getdbt.com/)

## Contribuição

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues e enviar pull requests.

## Autor
Material desenvolvido por [derycck](https://github.com/derycck).</br>
Base de dados cedida por [Andreza Leite](https://github.com/andrezaleite).

## Licença

Este projeto está licenciado sob a [Creative Commons Zero v1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/).

---

Espero que você aproveite o treinamento e aprenda bastante sobre DBT e DuckDB!