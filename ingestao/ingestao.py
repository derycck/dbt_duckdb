import os
from pathlib import Path

import pandas as pd

import duckdb


def adjust_encoding(stringa: str):
    """
    Ajusta o encoding de uma string.

    Args:
    stringa (str): String a ser ajustada.

    Returns:
    str: String ajustada.

    Raises:
    UnicodeDecodeError: Se a string não puder ser decodificada usando as
        codificações especificadas.
    Exception: Se ocorrer qualquer outra exceção durante o ajuste de
        codificação .
    process.
    """

    try:
        encoded = stringa.encode("latin1").decode("utf8")
        return encoded
    except UnicodeDecodeError:
        return stringa
    except Exception:
        return stringa


def preprocess(dataframe: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    Pré-processa o dataframe fornecido ajustando a codificação de uma coluna
    específica e renomeando todas as colunas para maiúsculas.
    Args:
        dataframe (pandas.DataFrame): The input dataframe.
        col_name (str): The name of the column to adjust the encoding.

    Returns:
        pandas.DataFrame: The preprocessed dataframe.
    """

    return dataframe.assign(
        **{col_name: lambda x: x[col_name].apply(adjust_encoding)}
    ).rename(columns=lambda x: x.upper())


def preprocess_url(
    url: str, file_name: str, column_to_correct: str
) -> dict[str, pd.DataFrame | str]:
    """
    Pré-processa um arquivo CSV a partir de uma URL e retorna um DataFrame e o
        nome da tabela.

    Args:
        url (str): A URL do arquivo CSV a ser baixado.
        file_name (str): O nome do arquivo, usado para derivar o nome da
            tabela.
        column_to_correct (str): O nome da coluna que precisa de correção no
            DataFrame.

    Returns:
        dict: Um dicionário contendo o DataFrame pré-processado ('df') e o nome
            da tabela ('table_name').
    """
    table_name: str = Path(file_name).stem.replace("Sample_", "").upper()
    df: pd.DataFrame = preprocess(
        pd.read_csv(url, encoding="utf8"),
        column_to_correct,
    )
    return {"df": df, "table_name": table_name}


def get_gestao_de_falta_por_mes(df_gestao_faltas):

    return (
        df_gestao_faltas.assign(ANO=lambda x: x["DAT_LIMITE"].str[:4])
        .assign(MES=lambda x: x["DAT_LIMITE"].str[5:7])
        .query('ANO=="2022"')
    )


def get_material_pivot(df_gestao_faltas):
    return (
        df_gestao_faltas.reindex(columns=["DESCR_MAT_FALT", "CENTRO_FORN"])
        .groupby(["DESCR_MAT_FALT", "CENTRO_FORN"])["CENTRO_FORN"]
        .count()
        .unstack(level=-1)
        .reset_index()
        .rename_axis(None, axis=1)
        .fillna(0)
    )


def main():
    """
    Ingerindo bases via URL no DuckDB.
    1. Remove qualquer arquivo DuckDB existente.
    2. Conecta-se ao banco de dados DuckDB.
    3. Cria o esquema "RAW".
    4. Ingere no DuckDB, as tabelas FACT_TOP_MATERIAL, GESTAO_FALTAS
    5. Cria novas bases através de transformações das anteriores
    6. Ingere 12 tabelas GESTAO_FALTAS_2022_MM e MATERIAL_PIVOT.
    """

    link_Sample_Fact_Top_Material = "https://github.com/andrezaleite/PES_Embraer_DBT/raw/main/data/Sample_Fact_Top_Material.zip"
    link_Sample_Gestao_Faltas = "https://github.com/andrezaleite/PES_Embraer_DBT/raw/main/data/Sample_Gestao_Faltas.zip"

    # Nome do arquivo DuckDB
    duckdb_file = Path(__file__).parent.absolute().parent / "db" / "dev.duckdb"
    duckdb_file.parent.mkdir(exist_ok=True)
    if duckdb_file.exists():
        print("Removendo arquivo DuckDB existente...\n")
        duckdb_file.unlink()
    # mensagem para informar local onde o banco de dados será salvo
    print(f"O banco de dados DuckDB será salvo em: {str(duckdb_file)}")

    # Conectar ao banco de dados DuckDB
    con = duckdb.connect(str(duckdb_file))

    # Criar schema "RAW"
    con.execute("CREATE SCHEMA RAW")

    print("\nInserindo dados no banco de dados DuckDB...")

    # Ingerir base top_material
    dict_return = preprocess_url(
        link_Sample_Fact_Top_Material,
        "Sample_Fact_Top_Material",
        "fTOP_DESCRICAO_ECODE",
    )
    df_fact_top_material: pd.DataFrame = dict_return["df"]
    table_name_fact_top_material = dict_return["table_name"]
    del dict_return

    print(f"Inserindo tabela raw.{table_name_fact_top_material}")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS raw.{table_name_fact_top_material} "
        + "AS SELECT * FROM df_fact_top_material"
    )
    # Ingerir base gestao_faltas
    dict_return = preprocess_url(
        link_Sample_Gestao_Faltas,
        "Sample_Gestao_Faltas",
        "DESCR_MAT_FALT",
    )
    df_gestao_faltas: pd.DataFrame = dict_return["df"]
    table_name_gestao_faltas = dict_return["table_name"]
    del dict_return
    print(f"Inserindo tabela raw.{table_name_gestao_faltas}")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS raw.{table_name_gestao_faltas} "
        + "AS SELECT * FROM df_gestao_faltas"
    )

    print("\nTransformando dados...")
    # Ingerir tabelas GESTAO_FALTAS_2022_MM
    df_gestao_faltas_2022_por_mes = get_gestao_de_falta_por_mes(
        df_gestao_faltas
    )
    for mes in range(1, 12 + 1):
        mes_str = f"{mes:02d}"
        df_mes = df_gestao_faltas_2022_por_mes.query(
            f'MES=="{mes_str}"'
        ).reset_index(drop=True)
        tb_name = f"GESTAO_FALTAS_2022_{mes_str}"
        cmd_str = (
            f"CREATE TABLE IF NOT EXISTS raw.{tb_name} AS SELECT * FROM df_mes"
        )
        print(f"Inserindo tabela raw.{tb_name}")
        con.execute(cmd_str)
    del df_gestao_faltas_2022_por_mes, df_mes, tb_name, cmd_str, mes_str

    # Ingerir tabela df_material_pivot
    df_material_pivot = get_material_pivot(df_gestao_faltas)
    tb_name = "MATERIAL_PIVOT"
    cmd_str = (
        f"CREATE TABLE IF NOT EXISTS raw.{tb_name} "
        + "AS SELECT * FROM df_material_pivot"
    )
    print(f"Inserindo tabela raw.{tb_name}")
    con.execute(cmd_str)

    # Fechar a conexão com o banco de dados
    con.close()


main()
