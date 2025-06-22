import duckdb
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi

DATA_DIR = Path('data')

RAW_DIR = DATA_DIR / 'raw'
CLEANED_DIR = DATA_DIR / 'cleaned'
DB_PATH = CLEANED_DIR / 'risklens.duckdb'

FILES = {
    'accepted':'accepted_2007_to_2018Q4.csv.gz',
    'rejected':'rejected_2007_to_2018Q4.csv.gz'
}

def ensure_directories(*dirs: Path):
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)

def download_files(api: KaggleApi, target_dir: Path, files: dict[str, str]):
    for label, fname in files.items():
        dest = target_dir / fname
        if not dest.exists():
            print(f'Downloading {fname}')
            api.dataset_download_file(
                'wordsforthewise/lending-club',
                file_name=fname,
                path=str(target_dir)
            )
            print(f'Downloaded {fname}')

def load_into_duckdb(db_path: Path, raw_dir: Path, files: dict[str, str]):
    con = duckdb.connect(database=str(db_path), read_only=False)
    for label, fname in files.items():
        file_path = raw_dir / fname
        table = f'raw_loans_{label}'
        print(f'Loading table "{table}" from {fname}')

        if label == 'accepted':
            con.execute(f'''
                create or replace table {table} as
                select * from read_csv_auto(
                    '{file_path}',
                    header=True,
                    types={{'id': 'varchar'}})
                        ''')
        else:
            con.execute(f'''
            create or replace table {table} as 
            select * from read_csv_auto(
                '{file_path}',
                header=True,
                types={{'Application Date':'timestamp'}})
                        ''')
    con.close()
    print('Loaded all tables to DuckDB')

def main():
    ensure_directories(RAW_DIR, CLEANED_DIR)
    api = KaggleApi()
    api.authenticate()
    download_files(api, RAW_DIR, FILES)
    load_into_duckdb(DB_PATH, RAW_DIR, FILES)


if __name__ == '__main__':
    main()