import urllib.request, os, gzip, shutil, re, logging, csv

from mdu.scryfall import Scryfall
from mdu.config.cache_17l import FILES, DRAFT_SET_SYMBOL_MAP

DATA_DIR = os.path.join('data', '17l-files')

def data_dir_path():
    """
    Where 17Lands data is stored. MDU_DATA_DIR environment variable is used, if it exists, otherwise the cwd is used
    """
    if project_dir := os.environ.get('MDU_PROJECT_DIR'):
        return os.path.join(project_dir, DATA_DIR)
    return DATA_DIR

def card_file_name(set_code):
    return f'{set_code}_card_file.csv'
    
def download_data_set(set_code, dataset_type, force_download=False):
    file_map = FILES[set_code][dataset_type]

    target_dir = data_dir_path()
    
    target_path = os.path.join(target_dir, file_map['target'])
    target_path_zipped = f'{target_path}.gz'

    if os.path.isfile(target_path) and not force_download:
        print(f'file {target_path} already exists, rerun with force_download=True to download')
        return
    
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)

    urllib.request.urlretrieve(file_map['source'], target_path_zipped)

    with gzip.open(target_path_zipped, 'rb') as f_in:
        with open(target_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out) 
    
    os.remove(target_path_zipped)

def write_card_file(draft_set_code):
    """
    Write a csv containing basic information about draftable cards, such as rarity, set symbol, color, mana cost, and type.

    Gets names from the 17lands headers and card information from a cache of Scryfall in local mongo.
    """
    data_dir = data_dir_path()
    draft_filepath = os.path.join(data_dir, FILES[draft_set_code]['draft']['target'])

    if not os.path.isfile(draft_filepath):
        logging.error(f'No draft file for set {draft_set_code}')
    
    columns = csv.DictReader(open(draft_filepath)).fieldnames
    pattern = '^pack_card_'
    names = [re.split(pattern, name)[1] for name in columns if re.search(pattern, name) is not None]

    card_attrs = ['name', 'set', 'rarity', 'color_identity_str', 'type', 'subtype', 'cmc']

    card_file_rows = ['card_index, ' + ', '.join(card_attrs) + '\n']
    sf = Scryfall(DRAFT_SET_SYMBOL_MAP[draft_set_code])
    for index, name in enumerate(names):
        card = sf.get_card(name)
        card_file_rows.append(str(index) + ', ' + card.attr_line(card_attrs) + '\n')
    
    card_filepath = os.path.join(data_dir, card_file_name(draft_set_code))
    with open(card_filepath, 'w') as f:
        f.writelines(card_file_rows)
