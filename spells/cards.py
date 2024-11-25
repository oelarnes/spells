import json
import urllib.request
from enum import StrEnum

from spells.enums import ColName


class CardAttr(StrEnum):
    NAME = ColName.NAME
    SET_CODE = ColName.SET_CODE
    COLOR = ColName.COLOR
    RARITY = ColName.RARITY
    COLOR_IDENTITY = ColName.COLOR_IDENTITY
    CARD_TYPE = ColName.CARD_TYPE
    SUBTYPE = ColName.SUBTYPE
    MANA_VALUE = ColName.MANA_VALUE
    MANA_COST = ColName.MANA_COST
    POWER = ColName.POWER
    TOUGHNESS = ColName.TOUGHNESS
    IS_BONUS_SHEET = ColName.IS_BONUS_SHEET
    IS_DFC = ColName.IS_DFC


MTG_JSON_TEMPLATE = "https://mtgjson.com/api/v5/{set_code}.json"


def _fetch_mtg_json(set_code: str) -> dict:
    request = urllib.request.Request(
        MTG_JSON_TEMPLATE.format(set_code=set_code),
        headers={"User-Agent": "spells-mtg/0.1.0"},
    )

    with urllib.request.urlopen(request) as f:
        draft_set_json = json.loads(f.read().decode("utf-8"))

    return draft_set_json


def _extract_value(set_code: str, name: str, card_dict: dict, field: CardAttr):
    match field:
        case CardAttr.NAME:
            return name
        case CardAttr.SET_CODE:
            return card_dict.get("setCode", "")
        case CardAttr.COLOR:
            return "".join(card_dict.get("colors", []))
        case CardAttr.RARITY:
            return card_dict.get("rarity", "")
        case CardAttr.COLOR_IDENTITY:
            return "".join(card_dict.get("colorIdentity", []))
        case CardAttr.CARD_TYPE:
            return " ".join(card_dict.get("types", []))
        case CardAttr.SUBTYPE:
            return " ".join(card_dict.get("subtypes", []))
        case CardAttr.MANA_VALUE:
            return str(card_dict.get("manaValue", ""))
        case CardAttr.MANA_COST:
            return card_dict.get("manaCost", "")
        case CardAttr.POWER:
            return str(card_dict.get("power", ""))
        case CardAttr.TOUGHNESS:
            return str(card_dict.get("toughness", ""))
        case CardAttr.IS_BONUS_SHEET:
            return str(card_dict.get("setCode", set_code) != set_code)
        case CardAttr.IS_DFC:
            return str(len(card_dict.get("otherFaceIds", [])) > 0)


def card_file_lines(draft_set_code, names):
    draft_set_json = _fetch_mtg_json(draft_set_code)
    set_codes = draft_set_json["data"]["booster"]["play"]["sourceSetCodes"]
    set_codes.reverse()

    card_data_map = {}
    for set_code in set_codes:
        if set_code != draft_set_code:
            card_data = _fetch_mtg_json(set_code)["data"]["cards"]
        else:
            card_data = draft_set_json["data"]["cards"]

        card_data.reverse()  # prefer front face for split cards
        face_name_cards = [item for item in card_data if "faceName" in item]
        card_data_map.update({item["faceName"]: item for item in face_name_cards})
        card_data_map.update({item["name"]: item for item in card_data})

    lines = [[field for field in CardAttr]]

    for name in names:
        lines.append(
            [
                _extract_value(draft_set_code, name, card_data_map.get(name, {}), field)
                for field in CardAttr
            ]
        )  # type: ignore

    return lines
