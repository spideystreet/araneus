import subprocess
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DATA_DIR = os.getenv("DATA_RAW_DIR", "data/raw")

DATASETS = {
    "elections_2020_t1_plus_1000": "https://static.data.gouv.fr/resources/elections-municipales-2020-resultats/20200525-133704/2020-05-18-resultats-communes-de-1000-et-plus.txt",
    "elections_2020_t2_plus_1000": "https://static.data.gouv.fr/resources/municipales-2020-resultats-2nd-tour/20200629-192435/2020-06-29-resultats-t2-communes-de-1000-hab-et-plus.txt",
    "rne_maires": "https://static.data.gouv.fr/resources/repertoire-national-des-elus-1/20251223-104211/elus-maires-mai.csv",
    "rne_conseillers_municipaux": "https://static.data.gouv.fr/resources/repertoire-national-des-elus-1/20251223-103336/elus-conseillers-municipaux-cm.csv",
    "budgets_communes": "https://data.ofgl.fr/api/explore/v2.1/catalog/datasets/ofgl-base-communes/exports/csv?use_labels=true"
}

def download_file(name, url):
    if "ofgl" in url:
        ext = "csv"
    else:
        ext = "csv" if "datasets/r/" in url else "txt"
    filename = os.path.join(DATA_DIR, f"{name}.{ext}")
    print(f"Downloading {name} from {url}...")
    try:
        # Using curl -L to follow redirects and -k to skip certificate validation if needed
        # We try without -k first, but -L is essential for data.gouv /datasets/r/ links
        subprocess.run(["curl", "-L", "-o", filename, url], check=True)
        print(f"Successfully downloaded to {filename}")
    except Exception as e:
        print(f"Error downloading {name}: {e}")

if __name__ == "__main__":
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    for name, url in DATASETS.items():
        download_file(name, url)
