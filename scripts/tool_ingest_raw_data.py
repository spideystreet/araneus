import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def ingest_data():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    
    files_to_ingest = [
        {
            "path": "data/raw/elections_2020_t1_plus_1000.txt",
            "table": "elections_2020_t1",
            "sep": "\t",
            "encoding": "latin1"
        },
        {
            "path": "data/raw/elections_2020_t2_plus_1000.txt",
            "table": "elections_2020_t2",
            "sep": ";",
            "encoding": "latin1"
        },
        {
            "path": "data/raw/rne_maires.txt",
            "table": "rne_maires",
            "sep": ";",
            "encoding": "utf-8"
        },
        {
            "path": "data/raw/rne_conseillers_municipaux.txt",
            "table": "rne_conseillers_municipaux",
            "sep": ";",
            "encoding": "utf-8"
        },
        {
            "path": "data/raw/budgets_communes.csv",
            "table": "budgets_communes",
            "sep": ";",
            "encoding": "utf-8"
        }
    ]
    
    for item in files_to_ingest:
        print(f"Ingesting {item['path']} into table {item['table']}...")
        if os.path.exists(item['path']):
            # Drop table with CASCADE to remove dependent views
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {item['table']} CASCADE"))
                conn.commit()
            
            # Use chunked ingestion for large files
            if item['table'] == 'elections_2020_t2':
                # T2 has variable candidates in columns, we take the first 30
                reader = pd.read_csv(item['path'], sep=item['sep'], encoding=item['encoding'], low_memory=False, usecols=range(30), chunksize=10000)
            else:
                reader = pd.read_csv(item['path'], sep=item['sep'], encoding=item['encoding'], low_memory=False, chunksize=10000)
            
            first_chunk = True
            for chunk in reader:
                if_exists = "replace" if first_chunk else "append"
                chunk.to_sql(item['table'], engine, if_exists=if_exists, index=False)
                first_chunk = False
            
            print(f"Successfully ingested {item['table']}")
        else:
            print(f"File {item['path']} not found, skipping.")

if __name__ == "__main__":
    ingest_data()
