import requests, tomllib
from functools import lru_cache
from importlib import resources

@lru_cache(maxsize=1)
def load_config():
    config_path = resources.files('mdu.config') / 'cache_17l.toml'
    with open(config_path, 'rb') as config:
        return tomllib.load(config)

def raw_data(set_code, dataset_type):
    config = load_config()
    repsonse = requests.get(config[set_code][dataset_type])
