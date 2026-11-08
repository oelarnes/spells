import pytest
import requests

from spells import cards


def card(number, name, **fields):
    return {"number": number, "name": name, **fields}


def basic(number, name):
    return card(number, name, supertypes=["Basic"], types=["Land"])


@pytest.fixture
def mtg_json(monkeypatch):
    def use(card_list):
        monkeypatch.setattr(
            cards, "_fetch_mtg_json", lambda code: {"data": {"cards": card_list}}
        )

    return use


def test_cards_numbered_before_the_basics_are_draftable(mtg_json):
    mtg_json(
        [
            card("2", "Shock"),
            card("1", "Opt"),
            basic("3", "Island"),
            card("4", "Starter Deck Card"),
            basic("5", "Mountain"),
        ]
    )
    assert cards.draftable_names("SET") == ["Opt", "Shock"]


def test_cards_after_the_basics_are_listed(mtg_json, capsys):
    mtg_json([card("1", "Opt"), basic("2", "Island"), card("3", "Starter Deck Card")])
    cards.draftable_names("SET")
    assert "Starter Deck Card" in capsys.readouterr().out


def test_alternate_printings_are_not_duplicates(mtg_json):
    mtg_json(
        [
            card("1", "Opt"),
            card("1b", "Opt", isAlternative=True),
            card("1†", "Opt", isAlternative=True),
            basic("2", "Island"),
        ]
    )
    assert cards.draftable_names("SET") == ["Opt"]


def test_a_name_numbered_twice_before_the_basics_is_an_error(mtg_json):
    mtg_json([card("1", "Opt"), card("2", "Opt"), basic("3", "Island")])
    with pytest.raises(ValueError, match="Opt"):
        cards.draftable_names("SET")


def test_multi_face_cards_use_17lands_names(mtg_json):
    mtg_json(
        [
            card("1", "Fire // Ice", layout="split", side="a", faceName="Fire"),
            card("1", "Fire // Ice", layout="split", side="b", faceName="Ice"),
            card("2", "Day // Night", layout="transform", side="a", faceName="Day"),
            card("2", "Day // Night", layout="transform", side="b", faceName="Night"),
            basic("3", "Island"),
        ]
    )
    assert cards.draftable_names("SET") == ["Fire // Ice", "Day"]


def test_a_set_mtgjson_lacks_is_unavailable(monkeypatch):
    response = requests.Response()
    response.status_code = 404

    def missing(code):
        raise requests.HTTPError(response=response)

    monkeypatch.setattr(cards, "_fetch_mtg_json", missing)
    with pytest.raises(cards.CardListUnavailable):
        cards.draftable_names("SET")
