import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from config import DB_CONFIG

caminho_csv = r"C:\git\world_population\data\world_population.csv"
tabela = "world_population"

try:
    df = pd.read_csv(caminho_csv)
    print(f"📄 Linhas lidas do CSV: {len(df)}")

    df = df.dropna(how="all")
    print(f"🧹 Linhas após limpar vazios: {len(df)}")

    conn_str = (
        f"DRIVER={{{DB_CONFIG['driver']}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
        "Trusted_Connection=no;"
    )

    print("🔍 Verificando existência do banco...")
    conn = pyodbc.connect(conn_str, autocommit=True)
    cursor = conn.cursor()
    cursor.execute(
        f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{DB_CONFIG['database']}') "
        f"CREATE DATABASE {DB_CONFIG['database']};"
    )
    conn.close()
    print(f"✅ Banco '{DB_CONFIG['database']}' garantido (existente ou criado).")

    connection_string = (
        f"mssql+pyodbc://{DB_CONFIG['username']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['server']}/{DB_CONFIG['database']}?driver=ODBC+Driver+17+for+SQL+Server"
    )
    engine = create_engine(connection_string)

    df.to_sql(tabela, engine, if_exists="replace", index=False)
    print(f"✅ Dados salvos na tabela '{tabela}' do banco '{DB_CONFIG['database']}'.")

except pyodbc.Error as e:
    print("❌ Erro ao conectar ou criar o banco de dados:")
    print(e)

except Exception as e:
    print("❌ Erro inesperado durante a execução:")
    print(e)
