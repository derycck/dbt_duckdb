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


def preprocess_(data_folder, file_name, column_to_correct):
    file_path = Path(data_folder) / file_name
    table_name = Path(file_name).stem.replace("Sample_", "").upper()
    df = preprocess(
        pd.read_csv(file_path, encoding="utf8"),
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
    Ingerindo arquivos CSV no DuckDB.
    1. Remove qualquer arquivo DuckDB existente.
    2. Conecta-se ao banco de dados DuckDB.
    3. Cria o esquema "RAW".
    4. Lista todos os arquivos CSV na pasta de dados especificada.
    5. Ingere no DuckDB, as tabelas FACT_TOP_MATERIAL, GESTAO_FALTAS,
        12 tabelas GESTAO_FALTAS_2022_MM e MATERIAL_PIVOT.
    """

    # Caminho para a pasta que contém os arquivos CSV)
    data_folder = Path(__file__).parent / "data"
    if not Path(data_folder).absolute().exists():
        raise FileNotFoundError(
            f"A pasta {data_folder} não existe.\n"
            + "Salve as bases de dados nela."
        )

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

    # Listar todos os arquivos CSV na pasta "data"
    csv_files = [
        f for f in os.listdir(data_folder) if f.lower().endswith(".csv")
    ]
    if len(csv_files) == 0:
        raise FileNotFoundError(
            "Nenhum arquivo CSV encontrado na pasta de dados.\n"
            + "Salve as bases de dados na pasta 'data'"
        )
    print("Arquivos CSV encontrados:")
    [print(f"- {file_name}") for file_name in csv_files]

    cols_to_correct = ["fTOP_DESCRICAO_ECODE", "DESCR_MAT_FALT"]

    # Iterar sobre cada arquivo CSV e inserir os dados no DuckDB
    print("\nInserindo dados no banco de dados DuckDB...")

    dict_return = preprocess_(
        data_folder, csv_files[0], "fTOP_DESCRICAO_ECODE"
    )
    df_fact_top_material = dict_return["df"]
    table_name_fact_top_material = dict_return["table_name"]
    del dict_return
    print(
        f"Inserindo tabela raw.{table_name_fact_top_material} - arquivo "
        + f"{csv_files[0]}"
    )
    con.execute(
        f"CREATE TABLE IF NOT EXISTS raw.{table_name_fact_top_material} "
        + "AS SELECT * FROM df_fact_top_material"
    )

    # preprocess_(data_folder, file_name, col_name):
    dict_return = preprocess_(data_folder, csv_files[1], "DESCR_MAT_FALT")
    df_gestao_faltas = dict_return["df"]
    table_name_gestao_Faltas = dict_return["table_name"]
    del dict_return
    print(
        f"Inserindo tabela raw.{table_name_gestao_Faltas} - arquivo "
        + f"{csv_files[1]}"
    )
    con.execute(
        f"CREATE TABLE IF NOT EXISTS raw.{table_name_gestao_Faltas} "
        + "AS SELECT * FROM df_gestao_faltas"
    )

    # con.close()
    # exit()
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
