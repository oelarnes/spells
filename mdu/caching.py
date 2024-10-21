import os

def cache_key(ddo, *args):
    set_code = ddo.set_code
    filter_spec = ddo.filter_str
    arg_str = str(args)
    
    hash_num = (set_code + filter_spec + arg_str).__hash__()
    return hex(hash_num)[3:]


def cache_exists(cache_key):
    pass


def read_cache(cache_key):
    pass


def write_cache(cache_key, df):
    pass

