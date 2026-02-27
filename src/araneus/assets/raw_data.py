import os
import subprocess
import pandas as pd
from sqlalchemy import create_engine, text
from dagster import asset, AssetExecutionContext
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = os.getenv("DATA_RAW_DIR", "data/raw")

def get_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

def download_and_ingest(name, url, table, sep, encoding, usecols=None):
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    # Mirroring the logic from tool_fetch_dgouv_data.py
    if "ofgl" in url:
        ext = "csv"
    else:
        ext = "csv" if "datasets/r/" in url else "txt"
    
    filename = os.path.join(DATA_DIR, f"{name}.{ext}")
    
    print(f"Downloading {name} from {url}...")
    # Using curl -L to follow redirects
    subprocess.run(["curl", "-L", "-o", filename, url], check=True)
    
    print(f"Ingesting {filename} into table {table}...")
    engine = get_engine()
    
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
        conn.commit()
    
    reader = pd.read_csv(
        filename, 
        sep=sep, 
        encoding=encoding, 
        low_memory=False, 
        usecols=usecols,
        dtype=str,
        chunksize=10000
    )
    
    first_chunk = True
    for chunk in reader:
        if_exists = "replace" if first_chunk else "append"
        chunk.to_sql(table, engine, if_exists=if_exists, index=False)
        first_chunk = False
    
    print(f"Successfully ingested {table}")

@asset(key_prefix="dgouv", group_name="raw_data", compute_kind="python")
def elections_2020_t1(context: AssetExecutionContext):
    url = "https://static.data.gouv.fr/resources/elections-municipales-2020-resultats/20200525-133704/2020-05-18-resultats-communes-de-1000-et-plus.txt"
    download_and_ingest("elections_2020_t1_plus_1000", url, "elections_2020_t1", "	", "latin1")

@asset(key_prefix="dgouv", group_name="raw_data", compute_kind="python")
def elections_2020_t2(context: AssetExecutionContext):
    url = "https://static.data.gouv.fr/resources/municipales-2020-resultats-2nd-tour/20200629-192435/2020-06-29-resultats-t2-communes-de-1000-hab-et-plus.txt"
    download_and_ingest("elections_2020_t2_plus_1000", url, "elections_2020_t2", ";", "latin1", usecols=range(30))

@asset(key_prefix="dgouv", group_name="raw_data", compute_kind="python")
def rne_maires(context: AssetExecutionContext):
    url = "https://static.data.gouv.fr/resources/repertoire-national-des-elus-1/20251223-104211/elus-maires-mai.csv"
    download_and_ingest("rne_maires", url, "rne_maires", ";", "utf-8")

@asset(key_prefix="dgouv", group_name="raw_data", compute_kind="python")
def rne_conseillers_municipaux(context: AssetExecutionContext):
    url = "https://static.data.gouv.fr/resources/repertoire-national-des-elus-1/20251223-103336/elus-conseillers-municipaux-cm.csv"
    download_and_ingest("rne_conseillers_municipaux", url, "rne_conseillers_municipaux", ";", "utf-8")

@asset(key_prefix="dgouv", group_name="raw_data", compute_kind="python")
def budgets_communes(context: AssetExecutionContext):
    url = "https://data.ofgl.fr/api/explore/v2.1/catalog/datasets/ofgl-base-communes/exports/csv?use_labels=true"
    download_and_ingest("budgets_communes", url, "budgets_communes", ";", "utf-8")
