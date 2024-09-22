import urllib.request, tomllib, os, gzip, shutil, re
import sqlite3

from functools import lru_cache
from importlib import resources

DEFAULT_DIR = os.path.join('data', '17l-files')
DB_DIR = 'db'
MAX_DRAFTABLE_CARDS = 400

@lru_cache(maxsize=1)
def load_config():
    config_path = resources.files('mdu.config') / 'cache_17l.toml'
    with open(config_path, 'rb') as config:
        return tomllib.load(config)

def download_data_set(set_code, dataset_type, force_download=False, target_dir=DEFAULT_DIR):
    config = load_config()
    filepath = config[set_code][dataset_type]
    target_path = os.path.join(target_dir, os.path.split(filepath)[1])
    unzipped_path = re.split('.gz$', target_path)[0]

    if os.path.isfile(unzipped_path) and not force_download:
        print(f'file {unzipped_path} already exists, rerun with force_download=True to download')
        return
    
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)

    urllib.request.urlretrieve(filepath, target_path)

    with gzip.open(target_path, 'rb') as f_in:
        with open(unzipped_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out) 
    
    os.remove(target_path)

def create_draft_table_script(num_draftable_cards=MAX_DRAFTABLE_CARDS):
    pack_card_headers = [f'pack_card_{n}' for n in range(num_draftable_cards)]
    pool_headers = [f'pool_{n}' for n in range(num_draftable_cards)]

    return f"""
CREATE TABLE draft_picks(
    expansion, 
    event_type, 
    draft_id, 
    draft_time, 
    rank, 
    event_match_wins, 
    event_match_losses, 
    pack_number, 
    pick_number,
    pick,
    pick_maindeck_rate,
    pick_sideboard_in_rate,
    {',\n    '.join(pack_card_headers)}, 
    {',\n    '.join(pool_headers)},
    user_n_games_bucket, 
    user_game_win_rate_bucket
)
    """

def write_to_sql(set_code, dataset_type, target_dir=DEFAULT_DIR):
    download_data_set(set_code, dataset_type, force_download=False, target_dir=target_dir)
    
    db_path = os.path.join(DB_DIR, 'sqlite.db')
    conn = sqlite3.connect(db_path)


