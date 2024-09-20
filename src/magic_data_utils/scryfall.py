import re
import json
import logging
import datetime
from functools import lru_cache
from importlib import resources

import requests
import pymongo

API_URL = 'https://api.scryfall.com/bulk-data/default_cards'

SEP_TYPE = 'Pipe'

CUBE_ATTRS = ['name', 'image_link', 'color_identity_name', 'type', 'cmc', 'subtypes', 'cube_sort_order']
SET_ATTRS = ['name_with_image_link', 'set_template_sort_order', 'color_identity_name', 'type', 'rarity', 'cmc', 'subtypes', 'power', 'toughness', 'oracle_one_line']

RARITY_ORDER = [
    'common',
    'uncommon',
    'rare',
    'mythic'
]

TYPE_ORDER = [
    "Creature",
    "Artifact Creature",
    "Enchantment Creature",
    "Planeswalker",
    "Instant",
    "Sorcery",
    "Artifact",
    "Enchantment",
    "Enchantment Artifact",
    "Land",
]

SET_TEMPLATE_RANK = ['rarity_rank', 'color_identity_rank', 'type_rank', 'cmc', 'name']
CUBE_RANK = ['color_identity_rank', 'type_rank', 'cmc', 'name']

@lru_cache(maxsize=10)
def get_config(property=None):
    config_path = resources.files('magic_data_utils.config') / 'scryfall_config.json'
    with open(config_path) as config:
        config = json.load(config)
    if property is None:
        return config
    else:
        return config[property]

def sort_order_string(rank_list):
    # if string, keep as is. if double, convert to 2 digit string, if
    def convert_el(el):
        if type(el) == type(1):
            return '{:02d}'.format(el)
        if type(el) == type(1.0):
            return '{:02.0f}'.format(el)
        return str(el)

    return ''.join([convert_el(el) for el in rank_list])


def rank_by_order(attr, order):
    return order.index(attr) if attr in order else len(order) + 1


def image_formula_from_card(card, format='normal'):
    return '=IMAGE("{}", 3)'.format(card['image_uris'][format])


def clean_oracle_text(text):
    text = text.replace('\n', '; ')
    reminder_text_re = r'\(.*?\)'
    return re.sub(reminder_text_re, '', text)


def shorten_oracle_text(text, to_length=64):
    text = clean_oracle_text(text)

    if len(text) > to_length:
        text = text[:to_length-3]+"..."
    return text


def strip_supertype(type_line):
    supertypes = ['Basic', 'Legendary', 'Ongoing', 'Snow', 'World']
    for type in supertypes:
        type_line = type_line.replace(type + ' ', '')
    return(type_line)


def image_tag_from_card(card, format='normal'):
    return '<img src={}></img>'.format(card['image_uris'][format])


def format_attr(attr):
    if type(attr)== type([]):
        return(''.join(attr))
    return str(attr)


def name_with_image_link(card):
    name = get_attr(card, 'name')
    image_link = get_attr(card, 'image_link')

    return '=HYPERLINK("{}","{}")'.format(image_link, name)


def mtgo_name(card):
    if 'card_faces' in card and card['layout'] == 'split':
        names = [card['card_faces'][i]['name'] for i in [0,1]]
        return '/'.join(names)
    return get_attr(card, 'name')


def get_attr(card, attr):
    color_name_map = get_config('color_name_map')
    card = dict(card)

    overrides = get_config('overrides')['attrs']
    if attr != 'name' and attr in overrides.get(get_attr(card, 'name'), {}):
        return overrides[get_attr(card, 'name')][attr]

    # take some attributes from the front face including name
    if 'card_faces' in card:
        front = dict(card['card_faces'][0])
        if card['layout'] in ('transform', 'flip', 'adventure'):
            card['name'] = front['name']
        front.update(card)
        card = front

    if attr == 'type':
        type_line = card['type_line']
        type_line = strip_supertype(type_line)
        return type_line.split(' — ')[0].split(' // ')[0]
    if attr == 'subtypes':
        if '—' in card['type_line']:
            return card['type_line'].split(' — ')[1].split(' // ')[0]
        else:
            return ''
    if attr == 'color_identity_name':
        col_id = format_attr(get_attr(card, 'color_identity'))
        if col_id in color_name_map:
            return color_name_map[format_attr(get_attr(card, 'color_identity'))]
        return col_id
    if attr == 'image_tag':
        return image_tag_from_card(card)
    if attr == 'image_formula':
        return image_formula_from_card(card)
    if attr == 'image_link':
        return card['image_uris']['large']
    if attr == 'oracle_one_line':
        return clean_oracle_text(get_attr(card, 'oracle_text'))
    if attr == 'short_oracle':
        return shorten_oracle_text(get_attr(card, 'oracle_text'))
    if attr == 'pt':
        if 'power' in card and 'toughness' in card:
            return '{}/{}'.format(card['power'], card['toughness'])
    if attr == 'type_rank':
        return rank_by_order(get_attr(card, 'type'), TYPE_ORDER)
    if attr == 'color_identity_rank':
        return rank_by_order(
            get_attr(card, 'color_identity_name'),
            get_config('color_name_order')
        )
    if attr == 'rarity_rank':
        return rank_by_order(
            get_attr(card, 'rarity'),
            RARITY_ORDER
        )
    if attr == 'set_template_sort_order':
        return sort_order_string([
            get_attr(card, rank_attr) for rank_attr in SET_TEMPLATE_RANK
        ])
    if attr == 'cube_sort_order':
        return sort_order_string([
            get_attr(card, rank_attr) for rank_attr in CUBE_RANK
        ])
    if attr == 'name_with_image_link':
        return name_with_image_link(card)
    if attr == 'mtgo_name':
        return mtgo_name(card)
    if attr in card:
        return card[attr]
    return ''


def get_attr_fmt(card, attr):
    return format_attr(get_attr(card, attr))

def get_client():
    return pymongo.MongoClient()

def get_attr_name(attr):
    map = {
        'cmc': 'CMC',
        'color_identity_name': 'Color Identity',
        'pt': 'P/T',
    }
    if attr in map:
        return map[attr]
    return attr.replace('_',' ').title()


def get_card_by_id(client, mtgo_id):
    cards_en = client.scryfall.cards_en

    query = {
        '$or': [
            {'mtgo_id': int(mtgo_id)},
            {'mtgo_foil_id': int(mtgo_id)}
        ]
    }

    card = cards_en.find_one(query)

    if card is None:
        error_message = 'No match for card id {}'.format(mtgo_id)
        logging.error(error_message)

    return card


def get_card(client, card_name, set=None):
    cards_en = client.scryfall.cards_en

    overrides = get_config('overrides')
    card_name = overrides['names'].get(card_name, card_name)

    base_query = {
        'set_type': {'$in': ['draft_innovation', 'expansion', 'commander', 'core', 'starter', 'funny']},
        'frame_effects': {'$nin': ['showcase', 'extendedart', 'borderless']},
    }

    if set is not None:
        base_query['set'] = set

    query = {'name': card_name}
    query.update(base_query)

    card = cards_en.find_one(query, sort=[('released_at', pymongo.ASCENDING)])

    if card is None:
        # separate this query since it will make it slower to combine
        query = {            
            'card_faces': {
                '$elemMatch': {
                    'name': card_name
                }
            },
        }
        query.update(base_query)

        card = cards_en.find_one(query, sort=[('released_at', pymongo.ASCENDING)])

    if card is None:
        query = {
            '$or': [
                {
                    'name': card_name,
                },
                {
                    'card_faces': {
                        '$elemMatch': {
                            'name': card_name
                        }
                    }
                }
            ],
            'set_type' : {
                '$ne': 'memorabilia'
            }
        }                

        if set is not None:
            query['set'] = set

        card = cards_en.find_one(query, sort=[('released_at', pymongo.ASCENDING), ('collector_number', pymongo.ASCENDING)])

    if card is None:
        error_message = 'No match for card name {}'.format(card_name)
        if set is not None:
            error_message = error_message + ' and set {}'.format(set)
        logging.error(error_message)

    return(card)


def form_query(query_params):
    elements = ['{}:{}'.format(it[0], it[1]) for it in query_params.items()]
    return '+'.join(elements)

def card_attr_line(client, card_input, attrs):
    split = card_input.strip('\n').split('|')
    card_name = split[0].strip()

    if len(split) >= 2:
        set = split[1]
    else:
        set = None

    card = get_card(client, card_name, set=set)
    if card is None:
        card_attr_line = [card_name if (attr == 'name') else '' for attr in attrs]
    else:
        card_attr_line = [get_attr_fmt(card, attr) for attr in attrs]

    return(join_line(card_attr_line))


def join_line(line):
    if SEP_TYPE == 'Pipe':
        return('|'.join(line))
    if SEP_TYPE == 'Quotes and Comma':
        return('"' + '", "'.join(line) + '"')
    if SEP_TYPE == 'Tab':
        return('\t'.join(line))


def clear_cache():
    client = pymongo.MongoClient()
    deleted = client.scryfall.cards_en.delete_many({})
    print(f'{deleted.deleted_count} cards deleted')

def fetch_cards(lang='en'):
    bulk_data_result = requests.get(API_URL)
    download_uri = bulk_data_result.json()['download_uri']

    download = requests.get(download_uri)
    cards = download.json()
    
    print('{} pulled'.format(len(cards)))

    return cards

def populate_cache():
    cards = fetch_cards()

    client = pymongo.MongoClient()
    clear_cache()
    client.scryfall.cards_en.insert_many(cards)
    client.scryfall.cards_en.create_index('mtgo_id')
    client.scryfall.cards_en.create_index('name')
    client.scryfall.import_metadata.update_one(
        {}, 
        {   
            '$set': {
                'as_of': f'{datetime.datetime.now(datetime.UTC).isoformat(timespec="milliseconds")}Z'
            }
        }, 
        upsert=True
    )

    print(f'{client.scryfall.cards_en.count_documents({})} cards inserted')
