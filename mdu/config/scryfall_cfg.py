OVERRIDES = {
    "attrs": {
        "Greater Gargadon": {
            "cmc": 1
        },
        "Ancestral Vision": {
            "cmc": 1
        },
        "Crashing Footfalls": {
            "cmc": 1
        },
        "Archangel Avacyn": {
            "color_identity": "W"
        },
        "Polluted Delta": {
            "color_identity": "BU"
        },
        "Flooded Strand": {
            "color_identity": "UW"
        },
        "Bloodstained Mire": {
            "color_identity": "BR"
        },
        "Wooded Foothills": {
            "color_identity": "GR"
        },
        "Windswept Heath": {
            "color_identity": "GW"
        },
        "Arid Mesa": {
            "color_identity": "RW"
        },
        "Scalding Tarn": {
            "color_identity": "RU"
        },
        "Misty Rainforest": {
            "color_identity": "GU"
        },
        "Verdant Catacombs": {
            "color_identity": "BG"
        },
        "Marsh Flats": {
            "color_identity": "BW"
        },
        "Commit // Memory": {
            "cmc": 4
        },
        "Depose // Deploy": {
            "cmc": 4
        },
        "Connive // Concoct": {
            "cmc": 4
        },
        "Grasslands": {
            "color_identity": "GW"
        },
        "Mountain Valley": {
            "color_identity": "GR"
        },
        "Bad River": {
            "color_identity": "BU"
        },
        "Rocky Tar Pit": {
            "color_identity": "BR"
        },
        "Flood Plain": {
            "color_identity": "UW"
        },
        "Vedalken Shackles": {
            "color_identity": "U"
        },
        "Shrine of Burning Rage": {
            "color_identity": "R"
        },
        "Loam Lion": {
            "color_identity": "GW"
        },
        "Kird Ape": {
            "color_identity": "GR"
        },
        "Garruk Relentless": {
            "color_identity": "G"
        },
        "Tamiyo, Inquisitive Student": {
            "color_identity": "U"
        },
        "Ajani, Nacatl Pariah": {
            "color_identity": "W"
        },
        "Grist, the Hunger Tide": {
            "color_identity": "G"
        },
        "Ral, Monsoon Mage": {
            "color_identity": "R"
        },
        "Sorin of House Markov": {
            "color_identity": "B"
        },
        "Status // Statue": {
            "cmc": 4
        },
        "Fire // Ice": {
            "cmc": 2
        },
        "Urborg, Tomb of Yawgmoth": {
            "color_identity": "B"
        },
        "Never // Return": {
            "cmc": 3
        },
        "Insult // Injury": {
            "cmc": 3
        },
        "Discover // Dispersal": {
            "cmc": 2
        },
        "Thrash // Threat": {
            "cmc": 4
        },
        "Life // Death": {
            "cmc": 2
        },
        "Expansion // Explosion": {
            "cmc": 2
        },
        "Wear // Tear": {
            "cmc": 2
        }
    },
    "names": {
        "Lim-Dul's Vault": "Lim-Dûl's Vault"
    }
}

COLOR_NAME_MAP = {
    "W":    "White",
    "U":    "Blue",
    "B":    "Black",
    "R":    "Red",
    "G":    "Green",
    "UW":   "Azorius",
    "BU":   "Dimir",
    "BR":   "Rakdos",
    "GR":   "Gruul",
    "GW":   "Selesnya",
    "BW":   "Orzhov",
    "BG":   "Golgari",
    "GU":   "Simic",
    "RU":   "Izzet",
    "RW":   "Boros",
    "BUW":  "Esper",
    "BRU":  "Grixis",
    "BGR":  "Jund",
    "GRW":  "Naya",
    "GUW":  "Bant",
    "BGW":  "Abzan",
    "BGU":  "Sultai",
    "GRU":  "Temur",
    "RUW":  "Jeskai",
    "BRW":  "Mardu",
    "GRUW": "Non-Black",
    "BGUW": "Non-Red",
    "BGRUW":"Five-Color",
    "":     "Colorless"
}
COLOR_NAME_ORDER = [
    "White",
    "Blue",
    "Black",
    "Red",
    "Green",
    "Azorius",
    "Dimir",
    "Rakdos",
    "Gruul",
    "Selesnya",
    "Orzhov",
    "Golgari",
    "Simic",
    "Izzet",
    "Boros",
    "Esper",
    "Grixis",
    "Jund",
    "Naya",
    "Bant",
    "Abzan",
    "Sultai",
    "Temur",
    "Jeskai",
    "Mardu",
    "Non-Black",
    "Non-Red",
    "Five-Color",
    "Colorless"
]

DRAFT_SET_SYMBOL_MAP = {
    "BLB": ["blb", "spg"],
    "OTJ": ["otj", "otp", "big", "spg"]
}


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