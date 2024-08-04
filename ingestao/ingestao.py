import os
from pathlib import Path

import duckdb
import pandas as pd


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
    except Exception as e:
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


def main():
    """
    Ingerindo arquivos CSV no DuckDB.
    1. Remove qualquer arquivo DuckDB existente.
    2. Conecta-se ao banco de dados DuckDB.
    3. Cria o esquema "RAW".
    4. Lista todos os arquivos CSV na pasta de dados especificada.
    5. Itera sobre cada arquivo CSV, pré-processa-o e insere os dados no DuckDB.
    """

    # Caminho para a pasta que contém os arquivos CSV
    data_folder = "data"
    # Nome do arquivo DuckDB
    duckdb_file = Path(".").absolute().parent / "db" / "dev.duckdb"
    duckdb_file.parent.mkdir(exist_ok=True)
    if duckdb_file.exists():
        print("Removendo arquivo DuckDB existente...\n")
        duckdb_file.unlink()

    # Conectar ao banco de dados DuckDB
    con = duckdb.connect(str(duckdb_file))

    # Criar schema "RAW"
    con.execute("CREATE SCHEMA RAW")

    # Listar todos os arquivos CSV na pasta "data"
    csv_files = [
        f for f in os.listdir(data_folder) if f.lower().endswith(".csv")
    ]
    print("Arquivos CSV encontrados:")
    [print(f"- {file_name}") for file_name in csv_files]

    cols_to_correct = ["fTOP_DESCRICAO_ECODE", "DESCR_MAT_FALT"]

    # Iterar sobre cada arquivo CSV e inserir os dados no DuckDB
    print("\nInserindo dados no banco de dados DuckDB...")
    for index, csv_file in enumerate(csv_files):
        file_path = Path(data_folder) / csv_file
        table_name = Path(csv_file).stem.replace("Sample_", "").upper()

        # Ler o arquivo CSV usando pandas
        df = preprocess(
            pd.read_csv(file_path, encoding="utf8"), cols_to_correct[index]
        )

        con.execute(
            f"CREATE TABLE IF NOT EXISTS raw.{table_name} AS SELECT * FROM df"
        )
        print(f"Arquivo {csv_file} inserido na tabela raw.{table_name}")

    # Fechar a conexão com o banco de dados
    con.close()


main()
