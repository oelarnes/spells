import json
import urllib.request
from enum import StrEnum

import polars as pl

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
    ORACLE_TEXT = ColName.ORACLE_TEXT
    CARD_JSON = ColName.CARD_JSON
    SCRYFALL_ID = ColName.SCRYFALL_ID
    IMAGE_URL = ColName.IMAGE_URL


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
    scryfall_id = card_dict.get("identifiers", {}).get("scryfallId", "")
    if scryfall_id:
        d1 = scryfall_id[0]
        d2 = scryfall_id[1]
        img_url = f"https://cards.scryfall.io/large/front/{d1}/{d2}/{scryfall_id}.jpg"
    else:
        img_url = ""
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
            return card_dict.get("manaValue", 0)
        case CardAttr.MANA_COST:
            return card_dict.get("manaCost", "")
        case CardAttr.POWER:
            return card_dict.get("power", None)
        case CardAttr.TOUGHNESS:
            return card_dict.get("toughness", None)
        case CardAttr.IS_BONUS_SHEET:
            return card_dict.get("setCode", set_code) != set_code
        case CardAttr.IS_DFC:
            return len(card_dict.get("otherFaceIds", [])) > 0
        case CardAttr.ORACLE_TEXT:
            return card_dict.get("text", "")
        case CardAttr.CARD_JSON:
            return card_dict.get("json", "")
        case CardAttr.SCRYFALL_ID:
            return scryfall_id
        case CardAttr.IMAGE_URL:
            return img_url

def card_df(draft_set_code: str, names: list[str]) -> pl.DataFrame:
    draft_set_json = _fetch_mtg_json(draft_set_code)
    booster_info = draft_set_json["data"]["booster"]

    booster_type = (
        "play"
        if "play" in booster_info
        else "draft"
        if "draft" in booster_info
        else list(booster_info.keys())[0]
    )
    set_codes = booster_info[booster_type]["sourceSetCodes"]
    set_codes.reverse()

    card_data_map = {}
    for set_code in set_codes:
        if set_code != draft_set_code:
            card_data = _fetch_mtg_json(set_code)["data"]["cards"]
        else:
            card_data = draft_set_json["data"]["cards"]

        card_data.reverse()  # prefer front face for split cards
        for item in card_data:
            item["json"] = json.dumps(item)

        face_name_cards = [item for item in card_data if "faceName" in item]
        card_data_map.update({item["faceName"]: item for item in face_name_cards})
        card_data_map.update({item["name"]: item for item in card_data})

    return pl.DataFrame(
        [
            {
                field: _extract_value(
                    draft_set_code, name, card_data_map.get(name, {}), field
                )
                for field in CardAttr
            }
            for name in names
        ]
    )
