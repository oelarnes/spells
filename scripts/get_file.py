import argparse
from mdu.cache_17l import load_config, download_data_set

parser = argparse.ArgumentParser(description='Download 17Lands public datasets')
parser.add_argument('sets', metavar='sets', nargs='+',
                    help='Sets to download. (Download all configured sets if none provided)')
parser.add_argument('-t', default='draft',
                    help='File type: draft (default), game, or replay')
parser.add_argument('-f', default='n', help='force download even if exists')

args = parser.parse_args()

config = load_config()

if len(args.sets) == 0:
    sets = list(config.keys())
else:
    sets = args.sets

file_type = args.t
force_download = (args.f.lower() == 'y')

for set_code in sets:
    print(f'Downloading {file_type} file for set {set_code}')
    download_data_set(set_code, file_type, force_download )
