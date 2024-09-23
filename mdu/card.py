import re

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

def _sort_order_string(rank_list):
    # if string, keep as is. if double, convert to 2 digit string, if
    def convert_el(el):
        if type(el) == type(1):
            return '{:02d}'.format(el)
        if type(el) == type(1.0):
            return '{:02.0f}'.format(el)
        return str(el)

    return ''.join([convert_el(el) for el in rank_list])

def _rank_by_order(attr, order):
    return order.index(attr) if attr in order else len(order) + 1

def _strip_supertype(type_line):
    supertypes = ['Basic', 'Legendary', 'Ongoing', 'Snow', 'World']
    for type in supertypes:
        type_line = type_line.replace(type + ' ', '')
    return(type_line)


def _format_attr(attr):
    if type(attr)== type([]):
        return(''.join(attr))
    return str(attr)

def _join_line(line, sep_type):
    if sep_type == 'Pipe':
        return('|'.join(line))
    if sep_type == 'Quotes and Comma':
        return('"' + '", "'.join(line) + '"')
    if sep_type == 'Tab':
        return('\t'.join(line))

class Card():
    IMAGE_SIZE_DEFAULT = 'normal'
    SHORT_ORACLE_LENGTH = 64
    SEP_TYPE = 'Pipe'

    def __init__(self, card_data, scryfall):
        self.__dict__ = dict(card_data)

        # take some attributes from the front face including name
        if len(self.card_faces) > 0:
            front = dict(self.card_faces[0])
            if self.layout in ('transform', 'flip', 'adventure'):
                self.__dict__['name'] = front['name']
            front.update(self.__dict__)
            self.__dict__ = front

        self._scryfall = scryfall

        self.image_link_large = self.image_uris['large']

        overrides = self._scryfall.config['overrides']['attrs'].get(self.name)
        for attr in overrides:
            setattr(self, attr, overrides[attr])
        

    @property
    def type(self):
        type_line = self.type_line
        type_line = _strip_supertype(type_line)

        return type_line.split(' — ')[0].split(' // ')[0]
    
    @property
    def type_rank(self):
        self.type_rank = _rank_by_order(self.type, TYPE_ORDER)
    
    @property
    def subtype(self):
        return self.type_line.split(' — ')[1].split(' // ')[0] if '—' in self.type_line else ''
    
    @property
    def color_identity_name(self):
        col_id = _format_attr(self.color_identity)
        return self._scryfall.config['color_name_map'].get(col_id, col_id)

    @property
    def color_identity_rank(self):
        return _rank_by_order(self.color_identity_name, self._scryfall.config['color_name_order'])
        
    @property
    def oracle_one_line(self):
        text = self.oracle_text.replace('\n', '; ')
        reminder_text_re = r'\(.*?\)'
        return re.sub(reminder_text_re, '', text)
    
    @property 
    def short_oracle(self):
        text = self.oracle_one_line
        return text[:self.SHORT_ORACLE_LENGTH-3]+"..." if len(text) > self.SHORT_ORACLE_LENGTH else text
    
    @property
    def pt(self):
        return '{}/{}'.format(self.power, self.toughness) if self.power != '' and self.toughness != '' else ''
    
    @property
    def image_tag(self):
        return f'<img src={self.image_uris[self.IMAGE_SIZE_DEFAULT]}></img>'
    
    @property
    def image_formula(self):
        return f'=IMAGE("{self.image_uris[self.IMAGE_SIZE_DEFAULT]}", 3)'
    
    @property
    def name_with_image_link(self):
        return '=HYPERLINK("{}","{}")'.format(self.image_linke, self.name)
    
    @property
    def image_link(self):
        return self.image_uris[self.IMAGE_SIZE_DEFAULT]

    @property
    def mtgo_name(self):
        if len(self.card_faces > 0) and self.layout == 'split':
            names = [self.card_faces[i]['name'] for i in [0,1]]
            return '/'.join(names)
        return self.name
    
    @property
    def rarity_rank(self):
        return _rank_by_order(self.rarity, RARITY_ORDER)
    
    def sort_order(self, ranking):
        _sort_order_string([getattr(self, rank_attr) for rank_attr in ranking])

    @property
    def set_template_sort_order(self):
        return self.sort_order(SET_TEMPLATE_RANK)
    
    @property
    def cube_sort_order(self):
        return self.sort_order(CUBE_RANK)
    
    def attr_line(self, attrs):
        return _join_line([_format_attr(getattr(self, attr) for attr in attrs)], self.SEP_TYPE)

    def __getattr__(self, attr):
        return ''
