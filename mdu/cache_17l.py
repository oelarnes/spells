import urllib.request, os, gzip, shutil, re, logging, csv

from mdu.scryfall import Scryfall
from mdu.config.scryfall_cfg import DRAFT_SET_SYMBOL_MAP
from mdu.config.cache_17l import config

DEFAULT_DIR = os.path.join('data', '17l-files')
    
def unzipped_path(zipped_path):
    return re.split('.gz$', zipped_path)[0]

def zipped_path(set_code, dataset_type, target_dir):
    return os.path.join(target_dir, os.path.split(config[set_code][dataset_type])[1])

def download_data_set(set_code, dataset_type, force_download=False, target_dir=DEFAULT_DIR):
    file_url = config[set_code][dataset_type]
    
    download_path = zipped_path(set_code, dataset_type, target_dir)
    final_path = unzipped_path(download_path)

    if os.path.isfile(final_path) and not force_download:
        print(f'file {final_path} already exists, rerun with force_download=True to download')
        return
    
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)

    urllib.request.urlretrieve(file_url, download_path)

    with gzip.open(download_path, 'rb') as f_in:
        with open(final_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out) 
    
    os.remove(download_path)

def write_card_file(draft_set_code, target_dir=DEFAULT_DIR):
    """
    Write a csv containing basic information about draftable cards, such as rarity, set symbol, color, mana cost, and type.

    Gets names from the 17lands headers and card information from a cache of Scryfall in local mongo.
    """
    filepath = unzipped_path(zipped_path(draft_set_code, 'draft', target_dir))

    if not os.path.isfile(filepath):
        logging.error(f'No draft file for set {draft_set_code}')
    
    columns = csv.DictReader(open(filepath)).fieldnames
    pattern = '^pack_card_'
    names = [re.split(pattern, name)[1] for name in columns if re.search(pattern, name) is not None]

    card_attrs = ['name', 'set', 'rarity', 'color_identity_str', 'type', 'subtype', 'cmc']

    card_file_rows = ['set_index, ' + ', '.join(card_attrs) + '\n']
    sf = Scryfall(DRAFT_SET_SYMBOL_MAP[draft_set_code])
    for index, name in enumerate(names):
        card = sf.get_card(name)
        card_file_rows.append(str(index) + ', ' + card.attr_line(card_attrs) + '\n')
    
    filename = f'{draft_set_code}_card_file.csv'
    filepath = os.path.join(target_dir, filename)
    with open(filepath, 'w') as f:
        f.writelines(card_file_rows)
