# dbt_duckdb
Treinamento de DBT com DuckDB. Introdução objetiva para você aprender na prática.

Bem-vindo! Vamos aprender sobre DBT (Data Build Tool) utilizando DuckDB. Este repositório contém alguns materiais úteis, exemplos e exercícios para ajudá-lo a aprender e dominar o uso do DBT com o DuckDB.

## O que é DBT?

DBT (Data Build Tool) é uma ferramenta de linha de comando open-source que auxilia analistas e engenheiros de dados a transformarem dados em seus data warehouses de maneira mais eficaz. Ele permite que você escreva consultas SQL modulares, teste suas transformações de dados e documente seus modelos de dados. O DBT foca na camada de transformação do processo ELT (Extract, Load, Transform), facilitando a construção e manutenção de pipelines de dados complexos.

## O que é DuckDB?

DuckDB é um sistema de gerenciamento de banco de dados OLAP SQL embutido. Ele é projetado para suportar cargas de trabalho de consultas analíticas de forma eficiente e pode ser incorporado em outras aplicações. O DuckDB é conhecido por seu alto desempenho, facilidade de uso e capacidade de lidar com grandes conjuntos de dados em uma única máquina.


## Estrutura do Repositório

Este repositório está organizado da seguinte forma:

- `ingestao/`: Contém o script para criar e ingerir os dados brutos no banco de dados.
- `db/`: Contém o banco de dados duckdb, que é apenas um arquivo.

Estrutura básica de um projeto dbt:
- `dbt_duckdb/`: O projeto DBT em si
- `dbt_duckdb/models/`: Contém os modelos DBT, que são arquivos SQL que definem as transformações.
- `dbt_duckdb/macros/`: Macros DBT personalizadas para estender a funcionalidade do DBT. (Não contemplado)
- `dbt_duckdb/tests/`: Casos de teste para garantir a correção das transformações de dados.

## Começando

Para começar, siga os passos abaixo:

1. **Clone o repositório**:
   ```bash
   git clone https://github.com/derycck/dbt_duckdb
   cd dbt_duckdb
   ```

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
- Declaração de metadados de modelo
- Gerar e visualizar documentação
	- Lineage: Visualização gráfica dos modelos
- Testes de modelo
	- Exemplo de teste genérico (unique)
- Comandos frequentes
- Materiais de suporte
- Alguns tópicos relevantes, mas não contemplados:
	- modelos python, macros, modelos incrementais, slow changing dimensions com snapshots, testes singulares em SQL, criação modelo SQL com jinja e iteração de ajuste com `dbt compile`


## Instalação do DBT

Em uma pasta vazia, vamos criar um ambiente virtual com as instalações necessárias para usar o DBT com duckdb e uma lib para manipulação de dados tabulares.

Para isso, execute o script `_install.bat` ou siga os passos abaixo:
```bash
python -m venv .venv
.venv\Scripts\activate # No linux ou mac use `source venv/bin/activate`
python -m pip install --upgrade pip
pip install dbt-duckdb pandas
```

## Ingestão da base de dados

- Baixe as duas bases de dados e salve em `./data`
  - [Sample_Fact_Top_Material.zip](https://github.com/andrezaleite/PES_Embraer_DBT/raw/main/data/Sample_Fact_Top_Material.zip)
  - [Sample_Gestao_Faltas.zip](https://github.com/andrezaleite/PES_Embraer_DBT/raw/main/data/Sample_Gestao_Faltas.zip)

Para criar o banco de dados:
- Abra um terminal com o ambiente virtual ativado
- Execute o arquivo `ingestao/ingestao.py`.

Repare que o banco de dados será criado em `db/dev.duckdb`.

## Criação do projeto DBT

Abra um terminal em uma pasta vazia e com o ambiente virtual ativado.


Execute o comando abaixo e siga as instruções no terminal
```shell
dbt init
```

As instruções podem com o passar das versões do DBT, mas essencialmente são:
- Digitar um nome para o projeto
- Escolher o banco de dados para a configuração de profile. Como o DBT foi instalado usando a extensão "dbt-duckdb", aparecerá apenas a opção do banco de dados duckdb. Assim, digite "1" e prossiga

## Configuração de profile

em dbt_duckdb/profiles.yml
```yml
default:
  outputs:
    dev:
      type: duckdb
      path: "../db/dev.duckdb"
      threads: 1
      schema: dbt

  target: dev
```


## Executar primeiro modelo
```shell
dbt run my_first_dbt_model
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

`models/_SOURCES.yml`
```yml
version: 2


sources:
  - name: raw
    description: schema raw
    tables:
      - name: FACT_TOP_MATERIAL
      - name: GESTAO_FALTAS
```
## Configuração de dbt_project

Vamos declarar que o tipo de declaração dos modelos serão determinados pela pasta onde eles se encontram.

Faça uma pequena edição ao final do arquivo, indicando que:
- os modelos criados na pasta models/staging , devem ser materializados como tabela no schema **staging**.
- os modelos criados na pasta models/serving , devem ser materializados como tabela no schema **serving**.

```yml
models:
  default:
    example:
      +materialized: view
    staging:
      +materialized: table
      +schema: STAGING
    serving:
      +materialized: table
      +schema: SERVING
```

## Criação de modelo com fonte no BD
Durante a construção de um sql model, para fazer referência a um modelo no banco de dados, usamos notação jinja, com a função source. Onde o primeiro argumento deve ser o schema e o segundo argumento deve ser o nome da coluna.

Para exemplificar, crie o modelo abaixo:

models/staging/dim_material.sql
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
select * from staging.dim_material
```

Agora vamos criar um modelo que use mais recursos do SQL.

A documentação do DBT recomenda adotar como boa prática, o uso de CTEs em substituição ao uso de subqueries.

Com CTEs, primeiro deve-se selecionar as colunas de cada fonte de dados, para ao final fazer a query final.

Essa boa prática busca facilitar a manutenabilidade das queries de transformação, uma vez que é preciso menos esforço mental seguindo esse estilo para interpretar as queries e identificar origem de cada coluna utilizada na transformação.

Crie o modelo abaixo:

`models/staging/dim_preco.sql`
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
select * from dbt_STAGING.dim_preco
```

## Criação de modelo com fonte em outro modelo
Para fazer referência a um modelo existente no DBT, usamos notação jinja com a função REF. O nome de modelo deve ser único em todo o projeto, assim não é necessário informar schema.

Apesar de um modelo DBT após ser materializado, possa ser referenciado através da função Source, existe algumas vantagens diferenciais em utilizar a função REF.
Dentre as principais, é possível citar a criação de uma relação de dependência explícita entre os modelos, o que permite ao DBT gerenciar a ordem de execução dos modelos automaticamente. Outra vantagem é a exposição gráfica dessa relação de dependência entre os modelos através do gráfico Lineage.

Crie o arquivo `models/serving/dim_projeto_stats.sql`
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
select * from dbt_SERVING.dim_projeto_stats
```

## Declaração de metadados de modelo

Os modelos são documentados através de arquivos YML, que também são usados para escrever declarar testes. Estes arquivos sustentam o microblog de documentação autogerado pelo DBT.
Apesar de ser possível criar um único arquivo yml para documentar todos os modelos, isso dificulta a manutenção do projeto na medida em que ele cresce.

Uma boa prática recomendada pela documentação oficial do DBT, é criar um arquivo de metadados por modelo. Preferencialmente com o prefixo `_model_`.

Crie o arquivo de metadados yml do modelo dim_preco:

`models/staging/_model_preco.yml`
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
`models/serving/_model_projeto_stats.yml`
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

`models/serving/_model_projeto_stats`
```yml
      - name: COUNT_PN_MAT_FALT
        data_type: string
        description: 'Contagem de materiais faltantes'
        data_tests:
          - unique
```

Execute o comando:
```shell
dbt tests -s dim_projeto_stats
```

Com isso no terminal será gerado um completo relatório de testes, informando inclusive que a coluna com o teste de valores únicos teve o teste falho.

Os testes genéricos contemplam `unique , not_null, accepted_values e relationships` , todos bem detalhados na documentação.

Além disso, é possível escrever testes singulares, baseado em queries SQL.

Para realizar um teste seguido da execução de modelo, o comando abaixo pode ser utilizado.
```shell
dbt build -s dim_projeto_stats
```
E para testar todos os modelos de nosso projeto:
```shell
dbt tests
```

Também é possível testar todos os modelos pertencentes a uma determinada Tag, mas esse será assunto para a próxima versão do treinamento.

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