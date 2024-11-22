import argparse
from spells.external import download_data_set, write_card_file
from spells.enums import View

parser = argparse.ArgumentParser(description='Download 17Lands public datasets')
parser.add_argument('sets', metavar='sets', nargs='+',
                    help='Sets to download. (Download all configured sets if none provided)')
parser.add_argument('-f', default='n', help='force download even if exists')

args = parser.parse_args()

if len(args.sets) == 0:
    raise ValueError("Supply set code")
else:
    sets = args.sets

file_type = args.t
force_download = (args.f.lower() == 'y')

for set_code in sets:
    print(f'Downloading {file_type} file for set {set_code}')
    download_data_set(set_code, View.DRAFT, force_download=force_download)
    download_data_set(set_code, View.GAME, force_download=force_download)
    write_card_file(set_code)
