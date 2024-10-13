import urllib.request, os, gzip, shutil, re, logging, csv
from enum import Enum

from mdu.scryfall import Scryfall
from mdu.config.mdu_cfg import DRAFT_SET_SYMBOL_MAP

DATA_DIR = os.path.join('data', '17l-files')

URL_TEMPLATE = "https://17lands-public.s3.amazonaws.com/analysis_data/{dataset_type}_data/{dataset_type}_data_public.{set_code}.{event_type}.csv.gz"


class EventType(Enum):
    PREMIER = "PremierDraft"
    TRADITIONAL = "TradDraft"

def data_dir_path():
    """
    Where 17Lands data is stored. MDU_DATA_DIR environment variable is used, if it exists, otherwise the cwd is used
    """
    if project_dir := os.environ.get('MDU_PROJECT_DIR'):
        return os.path.join(project_dir, DATA_DIR)
    return DATA_DIR


def data_file_path(set_code, dataset_type, event_type=EventType.PREMIER, zipped=False):
    if dataset_type == 'card':
        return os.path.join(data_dir_path(), f"{set_code}_card.csv")
    
    return os.path.join(data_dir_path(), "{set_code}_{event_type}_{dataset_type}.csv{suffix}".format(
        suffix=".gz" if zipped else "", 
        event_type=event_type.value,
        set_code=set_code, 
        dataset_type=dataset_type
    ))


def process_zipped_file(target_path_zipped, target_path):
    with gzip.open(target_path_zipped, 'rt', newline='') as f_in:
        with open(target_path, 'w', newline='') as f_out:
            # we are going to add an increasing draft_id index to the beginning of the line to facilitate distributed grouping by draft_id
            reader = csv.reader(f_in)
            writer = csv.writer(f_out)
            headers = next(reader)
            draft_id_loc = headers.index('draft_id')
            headers.insert(0, 'draft_id_idx')
            writer.writerow(headers)

            draft_id_idx = 0
            draft_id = None
            for row in reader:
                if row[draft_id_loc] != draft_id:
                    draft_id = row[draft_id_loc]
                    draft_id_idx += 1
                row.insert(0, draft_id_idx)
                writer.writerow(row)
    
    os.remove(target_path_zipped)
    

def download_data_set(set_code, dataset_type, event_type=EventType.PREMIER, force_download=False):
    target_dir = data_dir_path()
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)
    
    target_path_zipped = data_file_path(set_code, dataset_type, zipped=True)
    target_path = data_file_path(set_code, dataset_type)

    if os.path.isfile(target_path) and not force_download:
        logging.warning(f'file {target_path} already exists, rerun with force_download=True to download')
        return
    
    urllib.request.urlretrieve(URL_TEMPLATE.format(set_code=set_code, dataset_type=dataset_type, event_type=event_type.value), target_path_zipped)

    process_zipped_file(target_path_zipped, target_path)


def write_card_file(draft_set_code):
    """
    Write a csv containing basic information about draftable cards, such as rarity, set symbol, color, mana cost, and type.

    Gets names from the 17lands headers and card information from a cache of Scryfall in local mongo.
    """
    draft_filepath = data_file_path(draft_set_code, 'draft')

    if not os.path.isfile(draft_filepath):
        logging.error(f'No draft file for set {draft_set_code}')
    
    columns = csv.DictReader(open(draft_filepath)).fieldnames
    pattern = '^pack_card_' 

    names = (re.split(pattern, name)[1] for name in columns if re.search(pattern, name) is not None)

    card_attrs = ['name', 'set', 'rarity', 'color_identity_str', 'type', 'subtype', 'cmc']

    card_file_rows = [','.join(card_attrs) + '\n']
    sf = Scryfall(DRAFT_SET_SYMBOL_MAP[draft_set_code])
    for name in names:
        card = sf.get_card(name)
        card_file_rows.append(card.attr_line(card_attrs) + '\n')
    
    with open(data_file_path(draft_set_code, 'card'), 'w') as f:
        f.writelines(card_file_rows)
