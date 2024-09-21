import urllib.request, tomllib, os
from functools import lru_cache
from importlib import resources

DEFAULT_DIR='data/17l-files'

@lru_cache(maxsize=1)
def load_config():
    config_path = resources.files('mdu.config') / 'cache_17l.toml'
    with open(config_path, 'rb') as config:
        return tomllib.load(config)

def download_data_set(set_code, dataset_type, target_dir=DEFAULT_DIR):
    config = load_config()
    filepath = config[set_code][dataset_type]
    targetpath = os.path.join(target_dir, os.path.split(filepath)[1])
    
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)
    urllib.request.urlretrieve(filepath, targetpath)
